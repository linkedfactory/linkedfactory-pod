package io.github.linkedfactory.core.kvin.iceberg;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.ids.IdKind;
import io.github.linkedfactory.core.kvin.ids.IdStore;
import io.github.linkedfactory.core.kvin.records.KvinRecord;
import io.github.linkedfactory.core.kvin.records.KvinRecordStore;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;

import java.time.Duration;
import java.util.*;

/**
 * URI-based KVIN facade composing a numeric record store and a URI-ID store.
 * The location constructors own their components; injected components remain
 * owned by the caller. ID mappings are flushed before records are committed.
 */
public class KvinIceberg implements Kvin {
	private static final int BATCH_SIZE = 8192;
	private final KvinRecordStore recordStore;
	private final IdStore idStore;
	private final boolean owned;

	public KvinIceberg(String archiveLocation) {
		this(archiveLocation, null);
	}

	public KvinIceberg(String archiveLocation, Duration retentionPeriod) {
		if (retentionPeriod != null) {
			throw new UnsupportedOperationException("Iceberg retention cleanup is not implemented");
		}
		IcebergRecordStore records = new IcebergRecordStore(archiveLocation);
		try {
			idStore = new IcebergIdStore(archiveLocation, records.hasSnapshot());
		} catch (RuntimeException e) {
			records.close();
			throw e;
		}
		recordStore = records;
		owned = true;
	}

	/**
	 * Composes caller-owned stores. Closing this facade does not close either store.
	 */
	public KvinIceberg(KvinRecordStore recordStore, IdStore idStore) {
		this.recordStore = Objects.requireNonNull(recordStore);
		this.idStore = Objects.requireNonNull(idStore);
		owned = false;
	}

	public KvinRecordStore getRecordStore() {
		return recordStore;
	}

	public IdStore getIdStore() {
		return idStore;
	}

	private Map<URI, Long> ids(List<URI> uris, IdKind kind, boolean create) {
		if (idStore instanceof IcebergIdStore iceberg) {
			return iceberg.resolveIds(uris, kind, create);
		}
		Map<URI, Long> result = new HashMap<>();
		for (URI uri : uris) {
			if (!result.containsKey(uri)) {
				long id = idStore.getId(uri, kind, create);
				if (id != 0) {
					result.put(uri, id);
				}
			}
		}
		return result;
	}

	@Override
	public void put(KvinTuple... tuples) {
		put(Arrays.asList(tuples));
	}

	@Override
	public synchronized void put(Iterable<KvinTuple> tuples) {
		recordStore.put(() -> new Iterator<>() {
			final Iterator<KvinTuple> source = tuples.iterator();
			Iterator<KvinRecord> batch = Collections.emptyIterator();

			@Override
			public boolean hasNext() {
				if (!batch.hasNext() && source.hasNext()) {
					List<KvinTuple> input = new ArrayList<>(BATCH_SIZE);
					while (input.size() < BATCH_SIZE && source.hasNext()) {
						input.add(source.next());
					}
					Map<URI, Long> items = ids(input.stream().map(t -> t.item).distinct().toList(), IdKind.ITEM, true);
					Map<URI, Long> contexts = ids(input.stream().map(t -> context(t.context)).distinct().toList(), IdKind.CONTEXT, true);
					Map<URI, Long> properties = ids(input.stream().map(t -> t.property).distinct().toList(), IdKind.PROPERTY, true);
					idStore.flush();
					batch = input.stream().map(t -> new KvinRecord(items.get(t.item), contexts.get(context(t.context)),
							properties.get(t.property), t.time, t.seqNr, t.value)).iterator();
				}
				return batch.hasNext();
			}

			@Override
			public KvinRecord next() {
				if (!hasNext()) {
					throw new java.util.NoSuchElementException();
				}
				return batch.next();
			}
		});
	}

	private static URI context(URI context) {
		return context == null ? DEFAULT_CONTEXT : context;
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return fetchRows(List.of(item), property == null ? List.of() : List.of(property), context, null, null, limit, 0, null);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin,
			long limit, long interval, String op) {
		return fetch(List.of(item), property == null ? List.of() : List.of(property), context, end, begin, limit, interval, op);
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(List<URI> items, List<URI> properties, URI context, long end,
			long begin, long limit, long interval, String op) {
		return fetchRows(items, properties, context, end, begin, limit, interval, op);
	}

	private synchronized IExtendedIterator<KvinTuple> fetchRows(List<URI> items, List<URI> properties, URI context,
			Long end, Long begin, long limit, long interval, String op) {
		if (items.isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		Map<Long, URI> itemUris = reverse(ids(items, IdKind.ITEM, false));
		URI requestedContext = context(context);
		long contextId = idStore.getId(requestedContext, IdKind.CONTEXT, false);
		if (itemUris.isEmpty() || contextId == 0) {
			return NiceIterator.emptyIterator();
		}
		Map<Long, URI> propertyUris = reverse(ids(properties, IdKind.PROPERTY, false));
		if (!properties.isEmpty() && propertyUris.isEmpty()) {
			return NiceIterator.emptyIterator();
		}
		List<Long> itemIds = new ArrayList<>(itemUris.keySet());
		List<Long> propertyIds = new ArrayList<>(propertyUris.keySet());
		IExtendedIterator<KvinRecord> records = end == null
				? recordStore.fetch(itemIds.getFirst(), propertyIds.isEmpty() ? null : propertyIds.getFirst(), contextId, limit)
				: recordStore.fetch(itemIds, propertyIds, contextId, end, begin, limit, interval, op);
		return new NiceIterator<>() {
			final IExtendedIterator<KvinRecord> base = records;
			KvinTuple next;
			boolean closed;

			@Override
			public boolean hasNext() {
				if (next != null) {
					return true;
				}
				if (closed) {
					return false;
				}
				try {
					if (!base.hasNext()) {
						close();
						return false;
					}
					KvinRecord row = base.next();
					URI property = propertyUris.get(row.propertyId());
					if (property == null && idStore instanceof IcebergIdStore iceberg) {
						property = iceberg.cachedUri(row.propertyId(), IdKind.PROPERTY);
						if (property == null) {
							Set<Long> heads = base instanceof IcebergRecordStore.RecordIterator reader
									? reader.headProperties() : new HashSet<>();
							heads.add(row.propertyId());
							property = iceberg.resolveUris(heads, IdKind.PROPERTY).get(row.propertyId());
						}
					}
					if (property == null) {
						property = idStore.getUri(row.propertyId(), IdKind.PROPERTY);
					}
					next = new KvinTuple(itemUris.get(row.itemId()), property, requestedContext,
							row.time(), row.seqNr(), row.value());
					return true;
				} catch (RuntimeException e) {
					try {
						close();
					} catch (RuntimeException failure) {
						if (e != failure) {
							e.addSuppressed(failure);
						}
					}
					throw e;
				}
			}

			@Override
			public KvinTuple next() {
				if (!hasNext()) {
					throw new java.util.NoSuchElementException();
				}
				KvinTuple result = next;
				next = null;
				return result;
			}

			@Override
			public void close() {
				if (!closed) {
					closed = true;
					base.close();
				}
			}
		};
	}

	private static Map<Long, URI> reverse(Map<URI, Long> mappings) {
		Map<Long, URI> result = new HashMap<>();
		mappings.forEach((uri, id) -> result.put(id, uri));
		return result;
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		long itemId = idStore.getId(item, IdKind.ITEM, false);
		long contextId = idStore.getId(context(context), IdKind.CONTEXT, false);
		Long propertyId = property == null ? null : idStore.getId(property, IdKind.PROPERTY, false);
		if (itemId == 0 || contextId == 0 || (propertyId != null && propertyId == 0)) {
			return 0;
		}
		return recordStore.delete(itemId, propertyId, contextId, end, begin);
	}

	@Override
	public boolean delete(URI item, URI context) {
		long itemId = idStore.getId(item, IdKind.ITEM, false);
		long contextId = idStore.getId(context(context), IdKind.CONTEXT, false);
		return itemId != 0 && contextId != 0 && recordStore.delete(itemId, contextId);
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		long itemId = idStore.getId(item, IdKind.ITEM, false);
		long contextId = idStore.getId(context(context), IdKind.CONTEXT, false);
		if (itemId == 0 || contextId == 0) {
			return NiceIterator.emptyIterator();
		}
		try (IExtendedIterator<Long> properties = recordStore.properties(itemId, contextId)) {
			List<Long> ids = properties.toList();
			if (idStore instanceof IcebergIdStore iceberg) {
				Map<Long, URI> uris = iceberg.resolveUris(new HashSet<>(ids), IdKind.PROPERTY);
				return net.enilink.commons.iterator.WrappedIterator.create(ids.stream().map(uris::get).iterator());
			}
			return net.enilink.commons.iterator.WrappedIterator.create(ids.stream()
					.map(id -> idStore.getUri(id, IdKind.PROPERTY)).iterator());
		}
	}

	public void createBranch(String branchName) {
		if (recordStore instanceof IcebergRecordStore iceberg) {
			iceberg.createBranch(branchName);
		} else {
			throw new UnsupportedOperationException("Branches require an Iceberg record store");
		}
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return NiceIterator.emptyIterator();
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return NiceIterator.emptyIterator();
	}

	@Override
	public boolean addListener(KvinListener listener) {
		return false;
	}

	@Override
	public boolean removeListener(KvinListener listener) {
		return false;
	}

	@Override
	public void close() {
		if (owned) {
			try {
				((IcebergIdStore) idStore).close();
			} finally {
				recordStore.close();
			}
		}
	}
}

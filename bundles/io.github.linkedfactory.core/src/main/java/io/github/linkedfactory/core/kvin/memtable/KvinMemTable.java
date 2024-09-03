package io.github.linkedfactory.core.kvin.memtable;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinListener;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.parquet.records.KvinRecord;
import io.github.linkedfactory.core.kvin.util.Values;
import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import org.eclipse.rdf4j.sail.lmdb.config.LmdbStoreConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KvinMemTable implements Kvin {
	final KvinRecord END_MARKER = new KvinRecord();
	final Path path;
	ValueStore valueStore;
	TableFile currentTableFile;
	ExecutorService executor;
	CircularBuffer<KvinRecord> queue = new CircularBuffer<>(10000);
	int tableFileNr = 0;
	boolean async = true;

	KvinMemTable(Path path) throws IOException {
		this.path = path;
		Path valueStorePath = path.resolve("ids.dat");
		var config = new LmdbStoreConfig();
		config.setValueIDCacheSize(10000);
		this.valueStore = new ValueStore(valueStorePath.toFile(), config);
		this.executor = Executors.newSingleThreadExecutor();
	}

	void nextTableFile() throws IOException {
		if (this.currentTableFile == null || this.currentTableFile.size() > 20 * 1024 * 1024) {
			if (this.currentTableFile != null) {
				this.currentTableFile.close();
			}
			Path tableFilePath = path.resolve("entries_" + (tableFileNr++) + ".dat");
			this.currentTableFile = new TableFile(tableFilePath);
		}
	}

	public static byte[] encodeObject(Object valueObject) throws IOException {
		if (valueObject instanceof Record) {
			Record r = (Record) valueObject;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// marker for an object
			baos.write('O');
			int size = r.size();
			byte[] sizeBytes = new byte[Varint.calcLengthUnsigned(size)];
			Varint.writeUnsigned(sizeBytes, 0, size);
			baos.write(sizeBytes);
			for (Record element : r) {
				// write the property
				URI p = element.getProperty();
				byte[] content = p.toString().getBytes(StandardCharsets.UTF_8);
				int lengthBytes = Varint.calcLengthUnsigned(content.length);
				byte[] uriBytes = new byte[lengthBytes + content.length];
				Varint.writeUnsigned(uriBytes, 0, content.length);
				System.arraycopy(content, 0, uriBytes, 1, content.length);
				baos.write(uriBytes);

				// write the value
				baos.write(encodeObject(element.getValue()));
			}
			return baos.toByteArray();
		} else if (valueObject instanceof Object[]) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write('[');
			byte[] length = new byte[Varint.calcLengthUnsigned(((Object[]) valueObject).length)];
			Varint.writeUnsigned(length, 0, ((Object[]) valueObject).length);
			baos.write(length);
			for (Object v : (Object[]) valueObject) {
				baos.write(encodeObject(v));
			}
			return baos.toByteArray();
		} else if (valueObject instanceof URI) {
			URI uri = (URI) valueObject;
			byte[] content = uri.toString().getBytes(StandardCharsets.UTF_8);
			int lengthBytes = Varint.calcLengthUnsigned(content.length);
			byte[] uriBytes = new byte[1 + lengthBytes + content.length];
			uriBytes[0] = 'R';
			Varint.writeUnsigned(uriBytes, 1, content.length);
			System.arraycopy(content, 0, uriBytes, 1 + lengthBytes, content.length);
			return uriBytes;
		} else {
			return Values.encode(valueObject);
		}
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
	public void put(KvinTuple... tuples) {
		put(Arrays.asList(tuples));
	}

	@Override
	public void put(Iterable<KvinTuple> tuples) {
		var future = async ? executor.submit(() -> {
			while (true) {
				KvinRecord data = queue.remove();
				if (data == null) {
					Thread.onSpinWait();
				} else if (data == END_MARKER) {
					break;
				} else {
					nextTableFile();
					currentTableFile.put(data);
				}
			}
			return null;
		}) : null;
		try {
			valueStore.startTransaction(false);
			for (KvinTuple tuple : tuples) {
				KvinRecord record = new KvinRecord();

				record.itemId = valueStore.storeValue(valueStore.createIRI(tuple.item.toString()));
				record.propertyId = valueStore.storeValue(valueStore.createIRI(tuple.property.toString()));
				record.contextId = valueStore.storeValue(valueStore.createIRI(tuple.context.toString()));
				record.time = tuple.time;
				record.seqNr = tuple.seqNr;
				byte[] valueData = encodeObject(tuple.value);
				record.value = valueData;

				if (async) {
					while (!queue.add(record)) {
						Thread.onSpinWait();
					}
				} else {
					nextTableFile();
					currentTableFile.put(record);
				}
			}
			valueStore.commit();
		} catch (IOException e) {
			try {
				valueStore.rollback();
			} catch (IOException ex) {
				// ignore
			}
			throw new UncheckedIOException(e);
		} finally {
			if (async) {
				while (!queue.add(END_MARKER)) {
					Thread.onSpinWait();
				}
			}
		}
		if (async) {
			try {
				future.get();
			} catch (InterruptedException e) {
				// ignore
			} catch (ExecutionException e) {
				// ignore
			}
		}
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
		return null;
	}

	@Override
	public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
		return null;
	}

	@Override
	public long delete(URI item, URI property, URI context, long end, long begin) {
		return 0;
	}

	@Override
	public boolean delete(URI item, URI context) {
		return false;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> descendants(URI item, URI context, long limit) {
		return null;
	}

	@Override
	public IExtendedIterator<URI> properties(URI item, URI context) {
		return null;
	}

	@Override
	public void close() {
		try {
			currentTableFile.close();
			valueStore.close();
			if (executor != null) {
				executor.shutdown();
				executor = null;
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}

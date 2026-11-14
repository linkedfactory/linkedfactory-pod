package io.github.linkedfactory.core.rdf4j.vector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.MapRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.disk.OnDiskParallelGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.GraphIndexWriter;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JVectorIndex implements Closeable {
	private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();
	private static final int M = 16;
	private static final int EF_CONSTRUCTION = 100;
	private static final float NEIGHBOR_OVERFLOW = 1.2f;
	private static final float ALPHA = 1.2f;
	private static final int MIN_COMPACTION_BATCH = 1_000;
	private static final int MAX_COMPACTION_BATCH = 10_000;
	private static final double COMPACTION_FRACTION = 0.1;

	private final int dimension;
	private final VectorSimilarityFunction similarityFunction;
	private final Map<Integer, VectorFloat<?>> vectors = new ConcurrentHashMap<>();
	private final RandomAccessVectorValues ravv;
	private final GraphIndexBuilder builder;
	private final Map<String, Integer> iriToNode = new ConcurrentHashMap<>();
	private final Map<Integer, String> nodeToIri = new ConcurrentHashMap<>();
	private final AtomicInteger nextNodeId = new AtomicInteger();
	private final Set<Integer> pendingDeletions = new LinkedHashSet<>();

	public JVectorIndex(int dimension, VectorSimilarityFunction similarityFunction) {
		this.dimension = dimension;
		this.similarityFunction = similarityFunction;
		this.ravv = new MapRandomAccessVectorValues(vectors, dimension);
		BuildScoreProvider bsp = BuildScoreProvider.randomAccessScoreProvider(ravv, similarityFunction);
		this.builder = GraphIndexBuilder.builder(bsp, dimension, M)
				.withBeamWidth(EF_CONSTRUCTION)
				.withNeighborOverflow(NEIGHBOR_OVERFLOW)
				.withAlpha(ALPHA)
				.build();
	}

	public synchronized void upsert(String subjectIri, float[] vector) {
		if (vector.length != dimension) {
			throw new IllegalArgumentException("Expected vector of dimension " + dimension + ", got " + vector.length);
		}
		Integer existing = iriToNode.get(subjectIri);
		if (existing != null) {
			markDeleted(existing);
		}
		int node = nextNodeId.getAndIncrement();
		vectors.put(node, VTS.createFloatVector(vector));
		builder.addGraphNode(node, vectors.get(node));
		iriToNode.put(subjectIri, node);
		nodeToIri.put(node, subjectIri);
	}

	public synchronized void remove(String subjectIri) {
		Integer node = iriToNode.remove(subjectIri);
		if (node == null) {
			return;
		}
		markDeleted(node);
	}

	public synchronized void clear() {
		for (Integer node : iriToNode.values()) {
			builder.markNodeDeleted(node);
		}
		iriToNode.clear();
		nodeToIri.clear();
		builder.removeDeletedNodes();
		vectors.clear();
		pendingDeletions.clear();
	}

	public synchronized void compact() {
		flushPendingDeletions();
		builder.cleanup();
	}

	private void markDeleted(int node) {
		builder.markNodeDeleted(node);
		nodeToIri.remove(node);
		pendingDeletions.add(node);
		if (isEntryPoint(node)) {
			flushPendingDeletions();
			return;
		}
		int currentTotal = iriToNode.size() + pendingDeletions.size();
		int threshold = Math.max(MIN_COMPACTION_BATCH, (int) (currentTotal * COMPACTION_FRACTION));
		threshold = Math.min(threshold, MAX_COMPACTION_BATCH);
		if (pendingDeletions.size() >= threshold) {
			flushPendingDeletions();
		}
	}

	private boolean isEntryPoint(int node) {
		try (ImmutableGraphIndex.View view = builder.getGraph().getView()) {
			ImmutableGraphIndex.NodeAtLevel entry = view.entryNode();
			return entry != null && entry.node == node;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private void flushPendingDeletions() {
		if (pendingDeletions.isEmpty()) {
			return;
		}
		builder.removeDeletedNodes();
		for (int node : pendingDeletions) {
			vectors.remove(node);
		}
		pendingDeletions.clear();
	}

	public int size() {
		return iriToNode.size();
	}

	public Map<Integer, String> nodeToIriSnapshot() {
		return new ConcurrentHashMap<>(nodeToIri);
	}

	public List<VectorSearchHit> search(VectorSearchRequest request) throws IOException {
		ImmutableGraphIndex graph = builder.getGraph();
		Bits bits = Bits.ALL;
		if (request.getIriFilter() != null && !request.getIriFilter().isBlank()) {
			Integer onlyNode = iriToNode.get(request.getIriFilter());
			bits = onlyNode == null ? Bits.NONE : index -> index == onlyNode;
		}
		VectorFloat<?> queryVector = VTS.createFloatVector(request.getQueryVector());
		SearchScoreProvider ssp = DefaultSearchScoreProvider.exact(queryVector, similarityFunction, ravv);
		try (GraphSearcher searcher = new GraphSearcher(graph)) {
			SearchResult result = searcher.search(ssp, request.getTopK(), bits);
			List<VectorSearchHit> hits = new ArrayList<>(result.getNodes().length);
			for (SearchResult.NodeScore nodeScore : result.getNodes()) {
				String iri = nodeToIri.get(nodeScore.node);
				if (iri != null) {
					hits.add(new VectorSearchHit(iri, nodeScore.score));
				}
			}
			return hits;
		}
	}

	public void save(Path path) throws IOException {
		compact();
		ImmutableGraphIndex graph = builder.getGraph();
		try (GraphIndexWriter writer = new OnDiskParallelGraphIndexWriter.Builder(graph, path)
				.with(new InlineVectors(dimension))
				.build()) {
			var writerVectors = ravv.threadLocalSupplier();
			writer.write(Map.of(FeatureId.INLINE_VECTORS,
					nodeId -> new InlineVectors.State(writerVectors.get().getVector(nodeId))));
		}
	}

	public static OnDiskGraphIndex loadForSearch(Path path) throws IOException {
		ReaderSupplier readerSupplier = ReaderSupplierFactory.open(path);
		return OnDiskGraphIndex.load(readerSupplier);
	}

	public static List<VectorSearchHit> searchOnDisk(OnDiskGraphIndex graph, VectorSimilarityFunction similarityFunction,
			Map<Integer, String> nodeToIri, VectorSearchRequest request) throws IOException {
		try (GraphSearcher searcher = new GraphSearcher(graph)) {
			RandomAccessVectorValues graphRavv = (RandomAccessVectorValues) searcher.getView();
			VectorFloat<?> queryVector = VTS.createFloatVector(request.getQueryVector());
			SearchScoreProvider ssp = DefaultSearchScoreProvider.exact(queryVector, similarityFunction, graphRavv);
			SearchResult result = searcher.search(ssp, request.getTopK(), Bits.ALL);
			List<VectorSearchHit> hits = new ArrayList<>(result.getNodes().length);
			for (SearchResult.NodeScore nodeScore : result.getNodes()) {
				String iri = nodeToIri.get(nodeScore.node);
				if (iri != null) {
					hits.add(new VectorSearchHit(iri, nodeScore.score));
				}
			}
			return hits;
		}
	}

	@Override
	public void close() throws IOException {
		builder.close();
	}
}

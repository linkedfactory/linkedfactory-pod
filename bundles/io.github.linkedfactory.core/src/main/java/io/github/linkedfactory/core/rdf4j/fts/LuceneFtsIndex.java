package io.github.linkedfactory.core.rdf4j.fts;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LuceneFtsIndex implements AutoCloseable {
	public static final String SUBJECT_FIELD = "subject";
	private static final String ENTRY_FIELD = "_entry";
	private static final String ALL_TEXT_FIELD = "_all";
	private static final char SEP = '\u0001';

	private final Directory directory;
	private final StandardAnalyzer analyzer = new StandardAnalyzer();
	private final IndexWriter writer;
	private final SearcherManager searcherManager;
	private final Object lock = new Object();

	public record FieldValue(String field, String value, boolean iri, String context) {
	}

	public LuceneFtsIndex(Path indexDir) throws IOException {
		this.directory = FSDirectory.open(indexDir);
		this.writer = new IndexWriter(directory, new IndexWriterConfig(analyzer));
		writer.commit();
		this.searcherManager = new SearcherManager(writer, null);
	}

	public void applyAddRemove(Map<String, List<FieldValue>> added, Map<String, List<FieldValue>> removed) throws IOException {
		Set<String> subjects = new LinkedHashSet<>();
		subjects.addAll(added.keySet());
		subjects.addAll(removed.keySet());
		if (subjects.isEmpty()) {
			return;
		}
		synchronized (lock) {
			try (DirectoryReader reader = DirectoryReader.open(writer)) {
				IndexSearcher searcher = new IndexSearcher(reader);
				for (String subject : subjects) {
					List<FieldValue> current = readEntries(searcher, subject);
					List<FieldValue> toRemove = removed.getOrDefault(subject, List.of());
					current.removeIf(toRemove::contains);
					for (FieldValue entry : added.getOrDefault(subject, List.of())) {
						if (!current.contains(entry)) {
							current.add(entry);
						}
					}
					writeDocument(subject, current);
				}
			}
		}
	}

	public void clearAll() throws IOException {
		synchronized (lock) {
			writer.deleteAll();
		}
	}

	public void clearContexts(Set<String> contexts) throws IOException {
		if (contexts == null || contexts.isEmpty()) {
			return;
		}
		synchronized (lock) {
			try (DirectoryReader reader = DirectoryReader.open(writer)) {
				for (LeafReaderContext leafContext : reader.leaves()) {
					LeafReader leafReader = leafContext.reader();
					Bits liveDocs = leafReader.getLiveDocs();
					StoredFields storedFields = leafReader.storedFields();
					for (int docId = 0; docId < leafReader.maxDoc(); docId++) {
						if (liveDocs != null && !liveDocs.get(docId)) {
							continue;
						}
						Document doc = storedFields.document(docId);
						String subject = doc.get(SUBJECT_FIELD);
						List<FieldValue> entries = decodeEntries(doc.getValues(ENTRY_FIELD));
						boolean changed = entries.removeIf(entry -> entry.context() != null && contexts.contains(entry.context()));
						if (changed) {
							writeDocument(subject, entries);
						}
					}
				}
			}
		}
	}

	public void commit() throws IOException {
		synchronized (lock) {
			writer.commit();
		}
		searcherManager.maybeRefresh();
	}

	public List<FtsSearchHit> search(FtsSearchRequest request) throws Exception {
		IndexSearcher searcher = searcherManager.acquire();
		try {
			String fieldName = request.getField() == null || request.getField().isBlank() ? ALL_TEXT_FIELD : request.getField();
			SimpleQueryParser parser = new SimpleQueryParser(analyzer, fieldName);
			Query parsed = parser.parse(request.getKeywords());
			if (request.getBoost() != null) {
				parsed = new BoostQuery(parsed, request.getBoost().floatValue());
			}
			Query finalQuery = parsed;
			if (request.getIriFilter() != null && !request.getIriFilter().isBlank()) {
				finalQuery = new BooleanQuery.Builder()
						.add(parsed, BooleanClause.Occur.MUST)
						.add(new TermQuery(new Term(SUBJECT_FIELD, request.getIriFilter())), BooleanClause.Occur.FILTER)
						.build();
			}
			int limit = request.getLimit() > 0 ? request.getLimit() : 100;
			TopDocs topDocs = searcher.search(finalQuery, limit);
			Highlighter highlighter = null;
			if (request.isIncludeSnippet()) {
				highlighter = new Highlighter(new SimpleHTMLFormatter(), new QueryScorer(parsed));
				highlighter.setTextFragmenter(new SimpleFragmenter(80));
			}
			List<FtsSearchHit> hits = new ArrayList<>(topDocs.scoreDocs.length);
			StoredFields storedFields = searcher.storedFields();
			for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
				Document doc = storedFields.document(scoreDoc.doc);
				String iri = doc.get(SUBJECT_FIELD);
				String snippet = null;
				if (highlighter != null) {
					String text = fieldName.equals(ALL_TEXT_FIELD)
							? String.join(" ", doc.getValues(ALL_TEXT_FIELD))
							: doc.get(fieldName);
					if (text != null && !text.isEmpty()) {
						snippet = highlighter.getBestFragment(analyzer, fieldName, text);
					}
				}
				hits.add(new FtsSearchHit(iri, (double) scoreDoc.score, snippet));
			}
			return hits;
		} finally {
			searcherManager.release(searcher);
		}
	}

	@Override
	public void close() throws IOException {
		searcherManager.close();
		writer.close();
		directory.close();
	}

	private List<FieldValue> readEntries(IndexSearcher searcher, String subject) throws IOException {
		TopDocs top = searcher.search(new TermQuery(new Term(SUBJECT_FIELD, subject)), 1);
		if (top.scoreDocs.length == 0) {
			return new ArrayList<>();
		}
		Document doc = searcher.storedFields().document(top.scoreDocs[0].doc);
		return decodeEntries(doc.getValues(ENTRY_FIELD));
	}

	private void writeDocument(String subject, List<FieldValue> entries) throws IOException {
		Term key = new Term(SUBJECT_FIELD, subject);
		if (entries.isEmpty()) {
			writer.deleteDocuments(key);
		} else {
			writer.updateDocument(key, buildDocument(subject, entries));
		}
	}

	private Document buildDocument(String subject, List<FieldValue> entries) {
		Document doc = new Document();
		doc.add(new StringField(SUBJECT_FIELD, subject, Field.Store.YES));
		for (FieldValue entry : entries) {
			doc.add(new StoredField(ENTRY_FIELD, encode(entry)));
			if (entry.iri()) {
				doc.add(new StringField(entry.field(), entry.value(), Field.Store.YES));
			} else {
				doc.add(new TextField(entry.field(), entry.value(), Field.Store.YES));
				doc.add(new TextField(ALL_TEXT_FIELD, entry.value(), Field.Store.YES));
			}
		}
		return doc;
	}

	private static String encode(FieldValue entry) {
		return entry.field() + SEP + (entry.iri() ? '1' : '0') + SEP + (entry.context() == null ? "" : entry.context()) + SEP + entry.value();
	}

	private static List<FieldValue> decodeEntries(String[] raw) {
		List<FieldValue> result = new ArrayList<>(raw.length);
		for (String s : raw) {
			String[] parts = s.split(String.valueOf(SEP), 4);
			String context = parts[2].isEmpty() ? null : parts[2];
			result.add(new FieldValue(parts[0], parts[3], "1".equals(parts[1]), context));
		}
		return result;
	}
}

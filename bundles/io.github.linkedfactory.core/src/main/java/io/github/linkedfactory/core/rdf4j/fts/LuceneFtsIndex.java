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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
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
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class LuceneFtsIndex implements AutoCloseable {
	private static final Logger logger = LoggerFactory.getLogger(LuceneFtsIndex.class);
	public static final String SUBJECT_FIELD = "subject";
	private static final String ENTRY_FIELD = "_entry";
	private static final String ALL_TEXT_FIELD = "_all";
	private static final String CONTEXT_FIELD = "_context";
	private static final char SEP = '\u0001';

	private final Directory directory;
	private final StandardAnalyzer analyzer = new StandardAnalyzer();
	private final IndexWriter writer;
	private final SearcherManager searcherManager;
	private final Object lock = new Object();

	public record FieldValue(String field, String value, boolean iri, String context, String language,
			String datatype) {
	}

	public LuceneFtsIndex(Path indexDir) throws IOException {
		this.directory = FSDirectory.open(indexDir);
		if (!DirectoryReader.indexExists(directory)) {
			logger.warn("Creating a new, empty Lucene FTS index at {}. If an index previously existed at this "
					+ "location, its data is gone and nothing will be found by search until the affected models "
					+ "are re-imported.", indexDir);
		}
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
				Map<String, List<FieldValue>> currentBySubject = readEntriesForSubjects(reader, subjects);
				for (String subject : subjects) {
					List<FieldValue> current = currentBySubject.getOrDefault(subject, new ArrayList<>());
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
					Terms contextTerms = leafReader.terms(CONTEXT_FIELD);
					if (contextTerms == null) {
						continue;
					}
					Bits liveDocs = leafReader.getLiveDocs();
					StoredFields storedFields = leafReader.storedFields();
					Set<Integer> matchingDocIds = new TreeSet<>();
					TermsEnum termsEnum = contextTerms.iterator();
					for (String context : contexts) {
						if (termsEnum.seekExact(new BytesRef(context))) {
							PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
							int docId;
							while ((docId = postings.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
								if (liveDocs == null || liveDocs.get(docId)) {
									matchingDocIds.add(docId);
								}
							}
						}
					}
					for (int docId : matchingDocIds) {
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

	private Map<String, List<FieldValue>> readEntriesForSubjects(DirectoryReader reader, Set<String> subjects) throws IOException {
		Map<String, List<FieldValue>> result = new HashMap<>();
		for (LeafReaderContext leafContext : reader.leaves()) {
			LeafReader leafReader = leafContext.reader();
			Terms subjectTerms = leafReader.terms(SUBJECT_FIELD);
			if (subjectTerms == null) {
				continue;
			}
			Bits liveDocs = leafReader.getLiveDocs();
			StoredFields storedFields = leafReader.storedFields();
			TermsEnum termsEnum = subjectTerms.iterator();
			for (String subject : subjects) {
				if (result.containsKey(subject)) {
					continue;
				}
				if (termsEnum.seekExact(new BytesRef(subject))) {
					PostingsEnum postings = termsEnum.postings(null, PostingsEnum.NONE);
					int docId = postings.nextDoc();
					if (docId != PostingsEnum.NO_MORE_DOCS && (liveDocs == null || liveDocs.get(docId))) {
						Document doc = storedFields.document(docId);
						result.put(subject, decodeEntries(doc.getValues(ENTRY_FIELD)));
					}
				}
			}
		}
		return result;
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
			if (entry.context() != null) {
				doc.add(new StringField(CONTEXT_FIELD, entry.context(), Field.Store.NO));
			}
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
		return entry.field() + SEP + (entry.iri() ? '1' : '0') + SEP
				+ (entry.context() == null ? "" : entry.context()) + SEP
				+ (entry.language() == null ? "" : entry.language()) + SEP
				+ (entry.datatype() == null ? "" : entry.datatype()) + SEP
				+ entry.value();
	}

	private static List<FieldValue> decodeEntries(String[] raw) {
		List<FieldValue> result = new ArrayList<>(raw.length);
		for (String s : raw) {
			String[] parts = s.split(String.valueOf(SEP), 6);
			String context = parts[2].isEmpty() ? null : parts[2];
			String language = parts[3].isEmpty() ? null : parts[3];
			String datatype = parts[4].isEmpty() ? null : parts[4];
			result.add(new FieldValue(parts[0], parts[5], "1".equals(parts[1]), context, language, datatype));
		}
		return result;
	}
}

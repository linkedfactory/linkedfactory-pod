package io.github.linkedfactory.core.kvin.parquet;

import com.sun.jersey.core.util.Base64;
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;
import io.substrait.expression.FieldReference;
import io.substrait.expression.ImmutableFieldReference;
import io.substrait.extendedexpression.ExtendedExpressionProtoConverter;
import io.substrait.extendedexpression.ImmutableExpressionReference;
import io.substrait.extendedexpression.ImmutableExtendedExpression;
import io.substrait.extendedexpression.ProtoExtendedExpressionConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.proto.ExtendedExpression;
import io.substrait.relation.NamedScan;
import io.substrait.type.ImmutableNamedStruct;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class ArrowParquetTest {
	protected static final SimpleExtension.ExtensionCollection defaultExtensionCollection;

	static {
		try {
			defaultExtensionCollection = SimpleExtension.loadDefaults();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected TypeCreator R = TypeCreator.REQUIRED;

	io.substrait.proto.ExtendedExpression filterTable() {
		SubstraitBuilder b = new SubstraitBuilder(defaultExtensionCollection);
		Schema schema = ParquetHelpers.kvinTupleSchema;
		Collection<String> fieldNames = schema.getFields().stream().map(f -> f.name()).collect(Collectors.toList());
		Collection<Type> fieldTypes = schema.getFields().stream().map(f -> {
			var types = f.schema().isNullable() ? TypeCreator.NULLABLE : TypeCreator.REQUIRED;
			switch (f.schema().getType()) {
				case INT:
					return types.I32;
				case BOOLEAN:
					return types.BOOLEAN;
				case LONG:
					return types.I64;
				case BYTES:
					return types.BINARY;
				case FLOAT:
					return types.FP32;
				case DOUBLE:
					return types.FP64;
				case STRING:
					return types.STRING;
				case UNION:
					return types.BOOLEAN;
				default:
					throw new UnsupportedOperationException("Type not yet supported: " + f.schema().getType());
			}
		}).collect(Collectors.toList());
		// NamedScan scan = b.namedScan(List.of(schema.getName()), fieldNames, fieldTypes);
		var and = defaultExtensionCollection.getScalarFunction(SimpleExtension.FunctionAnchor.of("/functions_boolean.yaml", "and:bool"));
		var gte = defaultExtensionCollection.getScalarFunction(SimpleExtension.FunctionAnchor.of("/functions_comparison.yaml", "gte:any_any"));
		var lt = defaultExtensionCollection.getScalarFunction(SimpleExtension.FunctionAnchor.of("/functions_comparison.yaml", "lt:any_any"));

		var struct = Type.Struct.builder()
				.nullable(false)
				.fields(fieldTypes).build();
		var invokeGte = Expression.ScalarFunctionInvocation.builder()
				.outputType(TypeCreator.NULLABLE.BOOLEAN)
				.declaration(gte)
				.addAllArguments(List.of(FieldReference.StructField.of(1).constructOnRoot(struct),
						Expression.I64Literal.builder().value(1720512185231L).build()))
				.build();
		var invokeLt = Expression.ScalarFunctionInvocation.builder()
				.outputType(TypeCreator.NULLABLE.BOOLEAN)
				.declaration(lt)
				.addAllArguments(List.of(FieldReference.StructField.of(1).constructOnRoot(struct),
						Expression.I64Literal.builder().value(1720512189207L).build()))
				.build();
		var invokeAnd = Expression.ScalarFunctionInvocation.builder()
				.outputType(TypeCreator.NULLABLE.BOOLEAN)
				.declaration(and)
				.addAllArguments(List.of(invokeGte, invokeLt))
				.build();
		/*var filtered = NamedScan.builder()
				.from(scan)
				.filter(invokeAnd)
				.build();*/

		// var filtered = Filter.builder().input(scan).condition(invokeAnd).build();
		var reference = ImmutableExpressionReference.builder()
				.expression(invokeAnd)
				.addOutputNames("new-column")
				.build();
		var expr = new ExtendedExpressionProtoConverter().toProto(
				ImmutableExtendedExpression.builder()
						.addReferredExpressions(reference)
						.baseSchema(ImmutableNamedStruct.builder()
								.names(fieldNames).struct(struct).build())
						.build());
		System.out.println(expr);
		return expr;
		// var plan = b.plan(ImmutableRoot.builder().input(filtered).names(fieldNames).build());
		// return planProtoConverter.toProto(plan);
	}

	@Test
	public void testParseFilter() throws IOException {
		String base64EncodedSubstraitFilter =
				"Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEhoQCAIQAhoKbHQ6YW55X2F"
						+ "ueRo3ChwaGggCGgQKAhABIggaBhIECgISACIGGgQKAigUGhdmaWx0ZXJfaWRfbG93ZXJfdGhhbl8yMCIaCgJJRAoETkFNRRIOCgQqAhA"
						+ "BCgRiAhABGAI=";

		var expr = ExtendedExpression.parseFrom(Base64.decode(base64EncodedSubstraitFilter));
		System.out.println(expr);
	}

	@Test
	public void testReadData() throws Exception {
		var allocator = new RootAllocator(Long.MAX_VALUE);
		FileSystemDatasetFactory factory =
				new FileSystemDatasetFactory(
						allocator,
						NativeMemoryPool.getDefault(),
						FileFormat.PARQUET,
						getClass().getResource("/data__1.parquet").toURI().toString());

		final var filter = filterTable();
		final Dataset dataset = factory.finish();
		byte[] filterData = filter.toByteArray();
		ByteBuffer filterBuffer = ByteBuffer.allocateDirect(filterData.length);
		filterBuffer.put(filterData);
		ScanOptions options = new ScanOptions.Builder(32768)
				.columns(Optional.empty())
				.substraitFilter(filterBuffer)
				//.substraitProjection(filterBuffer)
				.build();
		try {
			for (int i = 0; i < 3; i++) {
				final Scanner scanner = dataset.newScan(options);
				long count = 0;
				long start = System.currentTimeMillis();

				try (ArrowReader reader = scanner.scanBatches()) {
					// System.out.println(scanner.schema().toString());
					while (reader.loadNextBatch()) {
						count += reader.getVectorSchemaRoot().getRowCount();
					}
				}
				System.out.println("count: " + count);
				System.out.println("reading took (arrow): " + (System.currentTimeMillis() - start));
				AutoCloseables.close(scanner);
			}
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		AutoCloseables.close(dataset, factory, allocator);
	}

	@Test
	public void testReadDataNative() throws Exception {
		FilterPredicate filter =
				gtEq(FilterApi.longColumn("time"), 1720512185231L);
		/* FilterPredicate filter = and(
				gtEq(FilterApi.longColumn("time"), 1720512185231L),
				lt(FilterApi.longColumn("time"), 1720512189207L)); */

		for (int i = 0; i < 20; i++) {
			var reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(getClass().getResource("/data__1.parquet").toString()), new Configuration()))
					.withDataModel(GenericData.get())
					.useStatsFilter()
					.withFilter(FilterCompat.get(filter))
					.build();

			long start = System.currentTimeMillis();
			long count = 0;
			GenericRecord record;
			while ((record = reader.read()) != null) {
				// do something
				count++;
			}
			System.out.println("count: " + count);
			System.out.println("reading took: " + (System.currentTimeMillis() - start));
			reader.close();
		}

	}

	@Test
	public void testReadDataDuckDb() throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:duckdb:");

		String query = "SELECT * FROM read_parquet('" + new File(getClass().getResource("/data__1.parquet").toURI()) + "') "
				+ "WHERE time >= 1720512185231"; // and time < 1720512189207";
		for (int i = 0; i < 20; i++) {
			long start = System.currentTimeMillis();
			long count = 0;
			Statement stmt = conn.createStatement();
			try (ResultSet rs = stmt.executeQuery(query)) {
				while (rs.next()) {
					count++;
				}
			}
			stmt.close();
			System.out.println("count: " + count);
			System.out.println("reading took: " + (System.currentTimeMillis() - start));
		}
	}

}

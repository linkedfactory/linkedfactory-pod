package io.github.linkedfactory.core.cli;

import io.github.linkedfactory.core.kvin.Kvin;
import io.github.linkedfactory.core.kvin.KvinTuple;
import io.github.linkedfactory.core.kvin.http.KvinHttp;
import io.github.linkedfactory.core.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.core.kvin.parquet.KvinParquet;
import io.github.linkedfactory.core.kvin.util.JsonFormatWriter;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import picocli.CommandLine;
import picocli.CommandLine.*;
import picocli.CommandLine.Model.CommandSpec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Command(name = "CLI",
		subcommands = {CLI.Copy.class, CLI.Fetch.class, CommandLine.HelpCommand.class},
		description = "Interact with KVIN stores")
public class CLI {
	@Option(names = {"-s", "--store"}, paramLabel = "<location>", required = true, description = "location URI of a KVIN store")
	protected String storeLocation;
	@Spec
	CommandSpec spec;

	public static void main(String[] args) {
		int exitCode = new CommandLine(new CLI()).execute(args);
		System.exit(exitCode);
	}

	private static Kvin createStore(String storeLocation) {
		URI locationUri = URIs.createURI(storeLocation);
		Path path = null;
		if (locationUri.scheme() != null && locationUri.scheme().startsWith("http")) {
			return new KvinHttp(storeLocation);
		} else if (locationUri.isFile()) {
			path = Paths.get(locationUri.toFileString());
		} else {
			path = Paths.get(storeLocation);
		}
		if (Files.isDirectory(path.resolve("ids"))) {
			return new KvinLevelDb(path.toFile());
		} else if (Files.isDirectory(path.resolve("metadata"))) {
			return new KvinParquet(path.toString());
		} else {
			throw new IllegalArgumentException("Invalid store location: " + storeLocation);
		}
	}

	@Command(description = "Retrieves all descendants of a given item")
	int descendants(@Parameters(paramLabel = "<item>", defaultValue = "", description = "the target item") String item) {
		URI itemUri = URIs.createURI(item);
		try (Kvin store = createStore(storeLocation)) {
			store.descendants(itemUri).forEach(d -> {
				System.out.println(d);
			});
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return 1;
		}
		return 0;
	}

	@Command(description = "Retrieves all properties of a given item")
	int properties(@Parameters(paramLabel = "<item>", description = "the target item") String item) {
		URI itemUri = URIs.createURI(item);
		try (Kvin store = createStore(storeLocation)) {
			store.properties(itemUri).forEach(p -> {
				System.out.println(p);
			});
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return 1;
		}
		return 0;
	}

	@Command(name = "copy", description = "Copies values from one store to another")
	static class Copy extends FetchBase implements Runnable {
		@Override
		public void run() {
		}
	}

	@Command(name = "fetch", description = "Fetches values for items")
	static class Fetch extends FetchBase implements Runnable {
		@Option(names = {"-i", "--interval"})
		Long interval;
		@Option(names = {"-o", "--op"})
		String op;

		@Override
		public void run() {
			URI itemUri = URIs.createURI(item);
			URI propertyUri = property == null ? null : URIs.createURI(property);
			URI contextUri = context == null ? Kvin.DEFAULT_CONTEXT : URIs.createURI(context);

			try (Kvin store = createStore(cli.storeLocation)) {
				IExtendedIterator<KvinTuple> tuples;
				if (op != null) {
					tuples = store.fetch(itemUri, propertyUri, contextUri, to != null ? to : KvinTuple.TIME_MAX_VALUE,
							from != null ? from : 0, limit != null ? limit : 0, interval != 0 ? interval : 0, op);
				} else {
					tuples = store.fetch(itemUri, propertyUri, contextUri, limit != null ? limit : 0);
				}
				JsonFormatWriter writer = new JsonFormatWriter(System.out, this.prettyPrint);
				while (tuples.hasNext()) {
					writer.writeTuple(tuples.next());
				}
				writer.close();
			} catch (Exception e) {
				System.err.println(e.getMessage());
				throw (e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
			}
		}
	}

	static abstract class FetchBase {
		@ParentCommand
		CLI cli;

		@Parameters(paramLabel = "<item>", description = "the item or *")
		String item;
		@Option(names = {"-p", "--property"})
		String property;
		@Option(names = {"-c", "--context"})
		String context;
		@Option(names = {"-f", "--from"})
		Long from;
		@Option(names = {"-t", "--to"})
		Long to;
		@Option(names = {"-l", "--limit"})
		Long limit;
		@Option(names = {"-P", "--pretty"})
		boolean prettyPrint;
	}
}
package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.util.Values;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Records {
	public static Object decodeRecord(byte[] data) {
		Record r = null;
		try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data)) {
			char type = (char) byteArrayInputStream.read();
			if (type == 'O') {
				int propertyLength = byteArrayInputStream.read();
				String property = new String(byteArrayInputStream.readNBytes(propertyLength), StandardCharsets.UTF_8);
				var value = decodeRecord(byteArrayInputStream.readAllBytes());
				if (r != null) {
					r.append(new Record(URIs.createURI(property), value));
				} else {
					r = new Record(URIs.createURI(property), value);
				}
			} else if (type == 'R') {
				int uriLength = byteArrayInputStream.read();
				String uri = new String(byteArrayInputStream.readNBytes(uriLength), StandardCharsets.UTF_8);
				return URIs.createURI(uri);
			} else {
				return Values.decode(data);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return r;
	}

	public static byte[] encodeRecord(Object record) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		if (record instanceof Record) {
			Record r = (Record) record;
			byteArrayOutputStream.write("O".getBytes(StandardCharsets.UTF_8));
			byte[] propertyBytes = r.getProperty().toString().getBytes();
			byteArrayOutputStream.write((byte) propertyBytes.length);
			byteArrayOutputStream.write(propertyBytes);
			byteArrayOutputStream.write(encodeRecord(r.getValue()));
		} else if (record instanceof URI) {
			URI uri = (URI) record;
			byte[] uriIndicatorBytes = "R".getBytes(StandardCharsets.UTF_8);
			byte[] uriBytes = new byte[uri.toString().getBytes().length + 1];
			uriBytes[0] = (byte) uri.toString().getBytes().length;
			System.arraycopy(uri.toString().getBytes(), 0, uriBytes, 1, uriBytes.length - 1);

			byte[] combinedBytes = new byte[uriIndicatorBytes.length + uriBytes.length];
			System.arraycopy(uriIndicatorBytes, 0, combinedBytes, 0, uriIndicatorBytes.length);
			System.arraycopy(uriBytes, 0, combinedBytes, uriIndicatorBytes.length, uriBytes.length);
			return combinedBytes;
		} else {
			return Values.encode(record);
		}
		return byteArrayOutputStream.toByteArray();
	}

}

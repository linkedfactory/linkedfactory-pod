package io.github.linkedfactory.core.kvin.parquet;

import io.github.linkedfactory.core.kvin.Record;
import io.github.linkedfactory.core.kvin.leveldb.EntryType;
import io.github.linkedfactory.core.kvin.util.Values;
import io.github.linkedfactory.core.kvin.util.Varint;
import net.enilink.komma.core.URI;
import net.enilink.komma.core.URIs;
import scala.Array;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Records {
	public static Object decodeRecord(ByteBuffer bb) throws IOException {
		byte type = bb.get();
		if (type == 'O') {
			int length = (int) Varint.readUnsigned(bb);
			Record dataObj = Record.NULL;
			while (length-- > 0) {
				int contentLength = (int) Varint.readUnsigned(bb);
				byte[] content = new byte[contentLength];
				bb.get(content);
				URI property = URIs.createURI(new String(content, StandardCharsets.UTF_8));
				Object value = decodeRecord(bb);
				dataObj = dataObj.append(new Record(property, value));
			}
			return dataObj;
		} else if (type == '[') {
			int length = (int) Varint.readUnsigned(bb);
			Object[] values = new Object[length];
			for (int i = 0; i < length; i++) {
				values[i] = decodeRecord(bb);
			}
			return values;
		} else if (type == 'R') {
			int uriLength = (int) Varint.readUnsigned(bb);
			byte[] content = new byte[uriLength];
			bb.get(content);
			return URIs.createURI(new String(content, StandardCharsets.UTF_8));
		} else {
			bb.position(bb.position() - 1);
			return Values.decode(bb);
		}
	}

	public static byte[] encodeRecord(Object record) throws IOException {
		if (record instanceof Record r) {
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
				baos.write(encodeRecord(element.getValue()));
			}
			return baos.toByteArray();
		} else if (record instanceof Object[]) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write('[');
			byte[] length = new byte[Varint.calcLengthUnsigned(((Object[]) record).length)];
			Varint.writeUnsigned(length, 0, ((Object[]) record).length);
			baos.write(length);
			for (Object v : (Object[]) record) {
				baos.write(encodeRecord(v));
			}
			return baos.toByteArray();
		} else if (record instanceof URI uri) {
			byte[] content = uri.toString().getBytes(StandardCharsets.UTF_8);
			int lengthBytes = Varint.calcLengthUnsigned(content.length);
			byte[] uriBytes = new byte[1 + lengthBytes + content.length];
			uriBytes[0] = 'R';
			Varint.writeUnsigned(uriBytes, 1, content.length);
			System.arraycopy(content, 0, uriBytes, 1 + lengthBytes, content.length);
			return uriBytes;
		} else {
			return Values.encode(record);
		}
	}

}

package com.radiadesign.catalina.session;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;

import org.apache.catalina.util.CustomObjectInputStream;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public class JavaSerializer implements Serializer {
	private final Log log = LogFactory.getLog(JavaSerializer.class);
	
	private ClassLoader loader;
	private MessageDigest digester = null; 
	
	public JavaSerializer() {
		try {
			digester = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			log.error("Unable to get MessageDigest instance for MD5");
		}
	}
	
	@Override
	public void setClassLoader(ClassLoader loader) {
		this.loader = loader;
	}
	
	@Override
	public byte[] serializeFrom(RedisSession session, SessionSerializationMetadata metadata) throws IOException {
		byte[] serialized = null;
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
			
			oos.writeObject(metadata);
			session.writeObjectData(oos);
			oos.flush();
			serialized = bos.toByteArray();
		}
		return serialized;
  }
	
	@Override
	public void deserializeInto(byte[] data, RedisSession session, SessionSerializationMetadata metadata) 
			throws IOException, ClassNotFoundException {
		try(BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(data));
				ObjectInputStream ois = new CustomObjectInputStream(bis, loader)) {
			SessionSerializationMetadata serializedMetadata = (SessionSerializationMetadata) ois.readObject();
			metadata.copyFieldsFrom(serializedMetadata);
			session.readObjectData(ois);
		}
	}

	@Override
	public byte[] attributesHashFrom(RedisSession session, List<String> ignorePropertyList) throws IOException {
		HashMap<String,Object> attributes = new HashMap<String,Object>();
		for (Enumeration<String> enumerator = session.getAttributeNames(); enumerator.hasMoreElements();) {
			String key = enumerator.nextElement();
			if (!ignorePropertyList.contains(key)) {
				attributes.put(key, session.getAttribute(key));
			}
		}
		
		byte[] serialized = null;
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
			oos.writeUnshared(attributes);
			oos.flush();
			serialized = bos.toByteArray();
		}
	    return digester.digest(serialized);
	}
}

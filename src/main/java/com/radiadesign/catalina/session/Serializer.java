package com.radiadesign.catalina.session;

import java.io.IOException;
import java.util.List;

public interface Serializer {
	void setClassLoader(ClassLoader loader);
	
	byte[] attributesHashFrom(RedisSession session, List<String> ignorePropertyList) throws IOException;
	
	byte[] serializeFrom(RedisSession session, SessionSerializationMetadata metadata) 
		throws IOException;
	
	void deserializeInto(byte[] data, RedisSession session, SessionSerializationMetadata metadata) 
		throws IOException, ClassNotFoundException;
}

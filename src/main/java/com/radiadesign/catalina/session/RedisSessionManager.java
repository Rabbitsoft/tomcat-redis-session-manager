package com.radiadesign.catalina.session;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.util.LifecycleSupport;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisSessionManager extends AbstractRedisSessionManager implements Lifecycle  {
	private static Log log = LogFactory.getLog(RedisSessionManager.class);

	class DeserializedSessionContainer {
		public final RedisSession session;
		public final SessionSerializationMetadata metadata;
		public DeserializedSessionContainer(RedisSession session, SessionSerializationMetadata metadata) {
			this.session = session;
			this.metadata = metadata;
		}
	}
	
	protected RedisSessionHandlerValve handlerValve;
	
	protected ThreadLocal<RedisSession> currentSession = new ThreadLocal<RedisSession>();
	protected ThreadLocal<String> currentSessionId = new ThreadLocal<String>();
	protected ThreadLocal<Boolean> currentSessionIsPersisted = new ThreadLocal<Boolean>();
	protected ThreadLocal<SessionSerializationMetadata> currentSessionSerializationMetadata = new ThreadLocal<>();
	
	/** 
	 * Serializes and deserializes session in redis. 
	 * By default {@link JavaSerializer}.
	 */
	protected Serializer serializer;
	
	/** 
	 * The lifecycle event support for this component.
	 */
	protected LifecycleSupport lifecycle = new LifecycleSupport(this);
	
	/** 
	 * SessionID to migrated to another session id.
	 */
	private String migrateSessionId; 
	
	/** 
	 * Contains an array with properties that should be ignored in 
	 * {@link Serializer#attributesHashFrom(RedisSession, List)}
	 */
	private List<String> ignorePropertyList = new ArrayList<String>();	
	
	protected JedisPool connectionPool;
	
	public int getDatabase() {
		return database;
	}
	
	public void setDatabase(int database) {
		this.database = database;
	}
	
	public String getIgnoredProperties() {
		return ignoredProperties;
	}

	public void setIgnoredProperties(String ignoredProperties) {
		this.ignoredProperties = ignoredProperties;
	}	
	
	@Override
	public int getRejectedSessions() {
		return 0;
	}

	public void setRejectedSessions(long i) {
	}

	public void setSerializationStrategyClass(String strategy) {
		this.serializationStrategyClass = strategy;
	}
	
	protected Jedis acquireConnection() {
		Jedis jedis = connectionPool.getResource();
		if (getDatabase() != 0) {
			jedis.select(getDatabase());
		}

		return jedis;
	}

	protected void returnConnection(Jedis jedis, Boolean error) {
		if (error) {
			connectionPool.returnBrokenResource(jedis);
		} else {
			connectionPool.returnResource(jedis);
		}
	}

	public void load() throws ClassNotFoundException, IOException {
	}
	
	public void unload() throws IOException {
	}
	
	@Override
	public void addLifecycleListener(LifecycleListener listener) {
		lifecycle.addLifecycleListener(listener);
	}

	@Override
	public LifecycleListener[] findLifecycleListeners() {
		return lifecycle.findLifecycleListeners();
	}

	@Override
	public void removeLifecycleListener(LifecycleListener listener) {
		lifecycle.removeLifecycleListener(listener);
	}	
	
	@Override
	protected synchronized void startInternal() throws LifecycleException {
	    super.startInternal();
	    
	    setState(LifecycleState.STARTING);

	    Boolean attachedToValve = false;
	    for (Valve valve : getContainer().getPipeline().getValves()) {
	    	if (valve instanceof RedisSessionHandlerValve) {
	    		this.handlerValve = (RedisSessionHandlerValve) valve;
	    		this.handlerValve.setRedisSessionManager(this);
	    		
	    		log.info("Attached to RedisSessionHandlerValve");
	    		attachedToValve = true;
	    		break;
	    	}
	    }

	    if (!attachedToValve) {
	    	String error = "Unable to attach to session handling valve; sessions cannot be saved after the request without the valve starting properly.";
	    	log.fatal(error);
	    	throw new LifecycleException(error);
	    }

	    try {
	    	initializeSerializer();
	    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
	    	log.fatal("Unable to load serializer", e);
	    	throw new LifecycleException(e);
	    }

	    log.info("Will expire sessions after " + getMaxInactiveInterval() + " seconds");
	    
	    String ignoreProps = getIgnoredProperties();
		if (ignoreProps != null) {
			for (String prop : ignoreProps.split(",")) {
				ignorePropertyList.add(prop.trim());
			}
		}	    
	    
		initializeDatabaseConnection();
		setDistributable(true);
	}	
	
	@Override
	protected synchronized void stopInternal() throws LifecycleException {
		if (log.isDebugEnabled()) {
			log.debug("Stopping");
		}
		
		setState(LifecycleState.STOPPING);
		try {
			connectionPool.destroy();
		} catch(Exception e) {
			// Do nothing.
	    }

		// Require a new random number generator if we are restarted
		super.stopInternal();
	}	
	
	@Override
	public Session createSession(String requestedSessionId) {
		RedisSession session = null;
		
		String sessionId = null;
		String jvmRoute = getJvmRoute();
	    
	    Boolean error = true;
	    Jedis jedis = null;
	    
	    try {
	    	jedis = acquireConnection();
	    	
	    	// Ensure generation of a unique session identifier.
	    	if (null != requestedSessionId) {
	    		sessionId = requestedSessionId;
	    		if (jvmRoute != null) {
	    			sessionId += '.' + jvmRoute;
	    		}
	    		
	    		if (jedis.setnx(sessionId.getBytes(), NULL_SESSION) == 0L) {
	    			sessionId = null;
	    		}
	    	} else {
	    		do {
	    			sessionId = generateSessionId();
	    			if (jvmRoute != null) {
	    				sessionId += '.' + jvmRoute;
	    			}
	    		} while (jedis.setnx(sessionId.getBytes(), NULL_SESSION) == 0L);
	    	}
	    	error = false;
	    	
	    	if (null != sessionId) {
		    	if (migrateSessionId != null) {
		    		try {
						session = (RedisSession) findSession(migrateSessionId);
						if (session != null) {
							jedis.del(session.getId());
							jedis.del(session.getId() + ":principal");
							
							session.setId(sessionId);
						}
					} catch (IOException e) {
						log.error("Failed to migrate session: " + e.getMessage());
					}
		    	} 
		    	
		    	if (session == null) {	    		
		    		session = (RedisSession)createEmptySession();
		    		session.setNew(true);
		    		session.setValid(true);
		    		session.setCreationTime(System.currentTimeMillis());
		    		session.setMaxInactiveInterval(getMaxInactiveInterval());
		    		session.setId(sessionId);
		    		session.tellNew();
		    	}
	    	}
	    	
	    	currentSessionSerializationMetadata.set(new SessionSerializationMetadata());
	    	currentSession.set(session);
	    	currentSessionId.set(sessionId);
	    	currentSessionIsPersisted.set(false);
	    	
	    	if (null != session) {
	    		try {
	    			error = saveInternal(jedis, session, true);
	    		} catch (IOException ex) {
	    			log.error("Error saving newly created session: " + ex.getMessage());
	    			currentSession.set(null);
	    			currentSessionId.set(null);
	    			session = null;
	    		}
	    	}
	    } finally {
	    	if (jedis != null) {
	    		returnConnection(jedis, error);
	    	}
	    }
	    return session;
	}	
	
	
	@Override
	public Session createEmptySession() {
		return new RedisSession(this);
	}
	
	
	@Override
	public void add(Session session) {
		try {
			save(session);
		} catch (IOException ex) {
			log.warn("Unable to add to session manager store: " + ex.getMessage());
			throw new RuntimeException("Unable to add to session manager store.", ex);
		}
	}
	
	@Override
	public Session findSession(String id) throws IOException {
		RedisSession session = null;
		if (id == null) {
			currentSessionIsPersisted.set(false);
			currentSession.set(null);
			currentSessionSerializationMetadata.set(null);
			currentSessionId.set(null);
		} else if (id.equals(currentSessionId.get())) {
			session = currentSession.get();
		} else {
			byte[] data = loadSessionDataFromRedis(id);
			if (data != null) {
				DeserializedSessionContainer container = sessionFromSerializedData(id, data);
				session = container.session;
				
				currentSession.set(session);
				currentSessionSerializationMetadata.set(container.metadata);
				currentSessionIsPersisted.set(true);
				currentSessionId.set(id);
				
			} else {
				currentSessionIsPersisted.set(false);
				currentSession.set(null);
				currentSessionSerializationMetadata.set(null);
				currentSessionId.set(null);
			}
		}
		return session;
	}	
	
	public void clear() {
		Jedis jedis = null;
		Boolean error = true;
		try {
			jedis = acquireConnection();
			jedis.flushDB();
			error = false;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}
	
	public byte[] loadSessionDataFromRedis(String id) throws IOException {
	    Jedis jedis = null;
	    Boolean error = true;

	    try {
	    	log.trace("Attempting to load session " + id + " from Redis");
	    	jedis = acquireConnection();
	    	
	    	byte[] data = jedis.get(id.getBytes());
	    	error = false;
	    	if (data == null) {
	    		log.trace("Session " + id + " not found in Redis");
	    	}
	    	return data;
	    } finally {
	    	if (jedis != null) {
	    		returnConnection(jedis, error);
	    	}
	    }
	}

	public DeserializedSessionContainer sessionFromSerializedData(String id, byte[] data) throws IOException {
		log.trace("Deserializing session " + id + " from Redis");
		if (Arrays.equals(NULL_SESSION, data)) {
			log.error("Encountered serialized session " + id + " with data equal to NULL_SESSION. This is a bug.");
			throw new IOException("Serialized session data was equal to NULL_SESSION");
		}
		
	    RedisSession session = null;
	    SessionSerializationMetadata metadata = new SessionSerializationMetadata();

	    try {
	    	session = (RedisSession) createEmptySession();
	    	
	    	serializer.deserializeInto(data, session, metadata);
	    	session.setId(id);
	    	session.setNew(false);
	    	session.setMaxInactiveInterval(getMaxInactiveInterval() * 1000);
	    	session.access();
	    	session.setValid(true);
	    	session.resetDirtyTracking();
	    	
	    	if (log.isTraceEnabled()) {
	    		log.trace("Session Contents [" + id + "]:");
	    		Enumeration<?> en = session.getAttributeNames();
	    		while(en.hasMoreElements()) {
	    			log.trace("  " + en.nextElement());
	    		}
	    	}
	    } catch (ClassNotFoundException ex) {
	    	log.fatal("Unable to deserialize into session", ex);
	    	throw new IOException("Unable to deserialize into session", ex);
	    }
	    return new DeserializedSessionContainer(session, metadata);
	}
	
	public void save(Session session) throws IOException {
		save(session, false);
	}
	
	public void save(Session session, boolean forceSave) throws IOException {
		Jedis jedis = null;
		Boolean error = true;
		try {
			jedis = acquireConnection();
			error = saveInternal(jedis, session, forceSave);
		} catch (IOException e) {
			throw e;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}
	
	protected boolean saveInternal(Jedis jedis, Session session, boolean forceSave) throws IOException {
		Boolean error = true;
		try {
			log.trace("Saving session " + session + " into Redis");
			RedisSession redisSession = (RedisSession) session;
			if (log.isTraceEnabled()) {
				log.trace("Session Contents [" + redisSession.getId() + "]:");
				Enumeration<?> en = redisSession.getAttributeNames();
				while(en.hasMoreElements()) {
					log.trace("  " + en.nextElement());
				}
			}
			
			byte[] binaryId = redisSession.getId().getBytes(), binaryAuthId = (redisSession.getId() + ":principal").getBytes();
			Boolean isCurrentSessionPersisted;
			
			SessionSerializationMetadata sessionSerializationMetadata = currentSessionSerializationMetadata.get();
			
			byte[] originalSessionAttributesHash = sessionSerializationMetadata.getSessionAttributesHash();
			byte[] sessionAttributesHash = null;
			
			if (forceSave
					|| redisSession.isDirty()
					|| null == (isCurrentSessionPersisted = this.currentSessionIsPersisted.get())
					|| !isCurrentSessionPersisted
					|| !Arrays.equals(originalSessionAttributesHash, 
							(sessionAttributesHash = serializer.attributesHashFrom(redisSession, ignorePropertyList)))) {
				
				log.trace("Save was determined to be necessary");
				if (sessionAttributesHash == null) {
					sessionAttributesHash = serializer.attributesHashFrom(redisSession, ignorePropertyList);
				}
				
				SessionSerializationMetadata updatedSerializationMetadata = new SessionSerializationMetadata();
				updatedSerializationMetadata.setSessionAttributesHash(sessionAttributesHash);
				
				jedis.set(binaryId, serializer.serializeFrom(redisSession, updatedSerializationMetadata));
				jedis.set(binaryAuthId,
					extractPrincipalName(redisSession).getBytes());
				
				redisSession.resetDirtyTracking();
				
				currentSessionSerializationMetadata.set(updatedSerializationMetadata);
				currentSessionIsPersisted.set(true);
			} else {
				log.trace("Save was determined to be unnecessary");
			}
			
			log.trace("Setting expire timeout on session [" + redisSession.getId() + "] to " + getMaxInactiveInterval());
			jedis.expire(binaryId, getMaxInactiveInterval());
			jedis.expire(binaryAuthId, getMaxInactiveInterval());
			
			error = false;
			return error;
		} catch (IOException e) {
			log.error(e.getMessage());
			throw e;
		}
	}	  
	
	/** 
	 * Customised implementation to extract spring security principal name to make 
	 * authentication via session possible. 
	 * 
	 * @param redisSession session
	 * @return empty string or username
	 */
	private String extractPrincipalName(RedisSession redisSession) {
		try {
			Object context = redisSession.getAttribute("SPRING_SECURITY_CONTEXT");
			if (context != null) {
				Method authMethod = context.getClass().getMethod("getAuthentication");
				if (authMethod != null) {
					Object authentication = authMethod.invoke(context);
					Method nameMethod = authentication.getClass().getMethod("getName");
					if (nameMethod != null) {
						return (String) nameMethod.invoke(authentication);
					}
				}
			}
		} catch (Exception ex) {
			log.warn("Failed to extract principal name:" + ex.getMessage());
		}
		return "";
	}

	@Override
	public void remove(Session session) {
		remove(session, false);
	}
	
	@Override
	public void remove(Session session, boolean update) {
		Jedis jedis = null;
		Boolean error = true;

		log.trace("Removing session ID : " + session.getId());
		try {
			jedis = acquireConnection();
			jedis.del(session.getId());
			error = false;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}
	
	public void beforeRequest(String sessionId) {
		migrateSessionId = sessionId;
	}
	
	public void afterRequest() {
		RedisSession redisSession = currentSession.get();
		migrateSessionId = null;
		
		if (redisSession != null) {
			try {
				if (redisSession.isValid()) {
					log.trace("Request with session completed, saving session " + redisSession.getId());
					save(redisSession, false);
				} else {
					log.trace("HTTP Session has been invalidated, removing :" + redisSession.getId());
					remove(redisSession);
				}
			} catch (Exception e) {
				log.error("Error storing/removing session", e);
			} finally {
				currentSession.remove();
				currentSessionId.remove();
				currentSessionIsPersisted.remove();
				log.trace("Session removed from ThreadLocal :" + redisSession.getIdInternal());
			}
		}
	}
	
	@Override
	public void processExpires() {
		// We are going to use Redis's ability to expire keys for session
		// expiration.

		// Do nothing.
	}
	
	private void initializeDatabaseConnection() throws LifecycleException {
		try {
			connectionPool = new JedisPool(
					getJedisConfig(), 
				getHost(), getPort(), getTimeout(), getPassword());
		} catch (Exception e) {
			e.printStackTrace();
			throw new LifecycleException("Error connecting to Redis", e);
		}
	}

	private void initializeSerializer() 
			throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		
		log.trace("Attempting to use serializer :" + serializationStrategyClass);
		serializer = (Serializer) Class.forName(serializationStrategyClass).newInstance();
		
		Loader loader = null;
		if (container != null) {
			loader = container.getLoader();
		}
		
		ClassLoader classLoader = null;
		if (loader != null) {
			classLoader = loader.getClassLoader();
		}
		serializer.setClassLoader(classLoader);
	}
}

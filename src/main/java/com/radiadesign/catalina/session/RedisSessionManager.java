package com.radiadesign.catalina.session;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisSessionManager extends AbstractRedisSessionManager implements Lifecycle  {
	private static Logger log = Logger.getLogger("RedisSessionManager");

	protected JedisPool connectionPool;

	/** 
	 * {@link RedisSessionHandlerValve}.
	 */
	protected RedisSessionHandlerValve handlerValve;
	
	/** 
	 * Serializes and deserializes session in redis. 
	 * By default {@link JavaSerializer}.
	 */
	protected Serializer serializer;
	
	/** 
	 * Current session deserialized.
	 */
	protected ThreadLocal<RedisSession> currentSession = new ThreadLocal<RedisSession>();

	/** 
	 * Current session id.
	 */
	protected ThreadLocal<String> currentSessionId = new ThreadLocal<String>();
	
	/**
	 * Deserialized {@link #currentSession} used to compare against current session for 
	 * attribute changes. Attributes can change by reference in which case the {@link RedisSession} 
	 * dirty check would not work.
	 */
	protected ThreadLocal<byte[]> serializedSession = new ThreadLocal<byte[]>();
	
	/** 
	 * Session has been persisted to redis or not.
	 */
	protected ThreadLocal<Boolean> currentSessionIsPersisted = new ThreadLocal<Boolean>();
	
	/** 
	 * Contains an array with properties that should be ignored in 
	 * {@link #isDirtyByReference(RedisSession)}.
	 */
	private List<String> ignorePropertyList = new ArrayList<String>();
	
	@Override
	public int getRejectedSessions() {
		// Essentially do nothing.
		return 0;
	}

	public void setRejectedSessions(long i) {
		// Do nothing.
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

	protected void returnConnection(Jedis jedis) {
		returnConnection(jedis, false);
	}

	public void load() throws ClassNotFoundException, IOException {
	}
	
	public void unload() throws IOException {
	}

	@Override
	public void startInternal() throws LifecycleException {
		super.startInternal();
		
		for (Valve valve : getContainer().getPipeline().getValves()) {
			if (valve instanceof RedisSessionHandlerValve) {
				this.handlerValve = (RedisSessionHandlerValve) valve;
				this.handlerValve.setRedisSessionManager(this);
				log.fine("Attached to RedisSessionHandlerValve");
				break;
			}
		}

		try {
			initializeSerializer();
		} catch (ClassNotFoundException e) {
			log.log(Level.SEVERE, "Unable to load serializer", e);
			throw new LifecycleException(e);
		} catch (InstantiationException e) {
			log.log(Level.SEVERE, "Unable to load serializer", e);
			throw new LifecycleException(e);
		} catch (IllegalAccessException e) {
			log.log(Level.SEVERE, "Unable to load serializer", e);
			throw new LifecycleException(e);
		}

		log.fine("Will expire sessions after " + getMaxInactiveInterval() + " seconds");

		initializeDatabaseConnection();
		setDistributable(true);
		
		String ignoreProps = getIgnoredProperties();
		if (ignoreProps != null) {
			for (String prop : ignoreProps.split(",")) {
				ignorePropertyList.add(prop.trim());
			}
		}
		setState(LifecycleState.STARTING);
	}

	@Override
	public void stopInternal() throws LifecycleException {
		super.stopInternal();
		
		try {
			connectionPool.destroy();
		} catch (Exception e) {
			// Do nothing.
		}
		setState(LifecycleState.STOPPING);
	}

	@Override
	public Session createSession(String sessionId) {
		log.info("Creating new session");
		
		RedisSession session = (RedisSession) createEmptySession();

		// Initialize the properties of the new session and return it
		session.setNew(true);
		session.setValid(true);
		session.setCreationTime(System.currentTimeMillis());
		session.setMaxInactiveInterval(getMaxInactiveInterval());
		
		String jvmRoute = getJvmRoute();

		Boolean error = true;
		Jedis jedis = null;

		try {
			jedis = acquireConnection();
			while (sessionId == null || jedis.get(sessionId.getBytes()) != null) {
				sessionId = generateSessionId();
				if (jvmRoute != null) {
					sessionId += '.' + jvmRoute;
				}
			}
			jedis.set(sessionId.getBytes(), NULL_SESSION); 
			error = false;

			session.setId(sessionId);
			session.tellNew();
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
			log.warning("Unable to add to session manager store: " + ex.getMessage());
			throw new RuntimeException("Unable to add to session manager store.", ex);
		}
	}

	@Override
	public Session findSession(String id) throws IOException {
		RedisSession session;

		if (id == null) {
			session = null;
			currentSessionIsPersisted.set(false);
		} else if (id.equals(currentSessionId.get())) {
			session = currentSession.get();
		} else {
			session = loadSessionFromRedis(id);
			if (session != null) {
				currentSessionIsPersisted.set(true);
			}
		}
		
		currentSession.set(session);
		currentSessionId.set(id);

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

	public RedisSession loadSessionFromRedis(String id) throws IOException {
		RedisSession session;

		Jedis jedis = null;
		Boolean error = true;
		try {
			log.fine("Attempting to load session " + id + " from Redis");
			
			jedis = acquireConnection();
			
			byte[] data = jedis.get(id.getBytes());
			if (!Arrays.equals(NULL_SESSION, data)) {
				serializedSession.set(data);
			}
			
			error = false;
			if (data == null) {
				log.fine("Session " + id + " not found in Redis");
				session = null;
			} else if (Arrays.equals(NULL_SESSION, data)) {
				throw new IllegalStateException(
						"Race condition encountered: attempted to load session[" 
					+ id + "] which has been created but not yet serialized.");
			} else {
				log.fine("Deserializing session " + id + " from Redis");
				session = (RedisSession) createEmptySession();
				serializer.deserializeInto(data, session);
				session.setId(id);
				session.setNew(false);
				session.setMaxInactiveInterval(getMaxInactiveInterval() * 1000);
				session.access();
				session.setValid(true);
				session.resetDirtyTracking();
				
				jedis.expire(id.getBytes(), getMaxInactiveInterval());
				jedis.expire((id + ":principal").getBytes(), 
					getMaxInactiveInterval());
				
				if (log.isLoggable(Level.FINE)) {
					log.fine("Session Contents [" + id + "]:");
					for (Object name : Collections.list(
							(Enumeration<?>)session.getAttributeNames())) {
						log.fine("  " + name);
					}
				}
			}

			return session;
		} catch (IOException e) {
			log.severe(e.getMessage());
			throw e;
		} catch (ClassNotFoundException ex) {
			log.log(Level.SEVERE, "Unable to deserialize into session", ex);
			throw new IOException("Unable to deserialize into session", ex);
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	public void save(Session session) throws IOException {
		Jedis jedis = null;
		Boolean error = true;
		try {
			log.fine("Saving session " + session + " into Redis");
			
			RedisSession redisSession = (RedisSession) session;
			if (log.isLoggable(Level.FINE)) {
				log.fine("Session Contents [" + redisSession.getId() + "]:");
				for (Object name : Collections.list(
						(Enumeration<?>)redisSession.getAttributeNames())) {
					log.fine("  " + name);
				}
			}
			Boolean sessionIsDirty = redisSession.isDirty();
			redisSession.resetDirtyTracking();
			
			byte[] binaryId = redisSession.getId().getBytes(), binaryAuthId = (redisSession.getId() + ":principal").getBytes();

			jedis = acquireConnection();
			if (sessionIsDirty || currentSessionIsPersisted.get() != true || isDirtyByReference(redisSession)) {
				jedis.set(binaryId, serializer.serializeFrom(redisSession));
				jedis.set(binaryAuthId, 
					extractPrincipalName(redisSession).getBytes());
			}

			currentSessionIsPersisted.set(true);
			log.fine("Setting expire timeout on session [" 
				+ redisSession.getId() + "] to " + getMaxInactiveInterval());
			
			jedis.expire(binaryId, getMaxInactiveInterval());
			jedis.expire(binaryAuthId, getMaxInactiveInterval());
			
			error = false;
		} catch (IOException e) {
			log.severe(e.getMessage());

			throw e;
		} finally {
			if (jedis != null) {
				returnConnection(jedis, error);
			}
		}
	}

	@Override
	public void remove(Session session) {
		remove(session, false);
	}
	
	@Override
	public void remove(Session session, boolean update) {
		Jedis jedis = null;
		Boolean error = true;

		log.fine("Removing session ID : " + session.getId());
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

	public void afterRequest() {
		RedisSession redisSession = currentSession.get();
		if (redisSession != null) {
			currentSession.remove();
			currentSessionId.remove();
			currentSessionIsPersisted.remove();
			serializedSession.remove();			
			
			log.fine("Session removed from ThreadLocal :" 
				+ redisSession.getIdInternal());
		}
	}

	@Override
	public void processExpires() {
		// We are going to use Redis's ability to expire keys for session
		// expiration.

		// Do nothing.
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
				Method authenticationMethod = context.getClass().getMethod("getAuthentication");
				if (authenticationMethod != null) {
					Object authentication = authenticationMethod.invoke(context);
					
					Method nameMethod = authentication.getClass().getMethod("getName");
					if (nameMethod != null) {
						return (String) nameMethod.invoke(authentication);
					}
				}
				
			}
		} catch (Exception ex) {
			log.warning("Failed to extract principal name:" + ex.getMessage());
		}
		return "";
	}
	
	private boolean isDirtyByReference(RedisSession redisSession) { 
		if (serializedSession.get() == null) {
			return false;
		}
		try {
			HttpSession initialSession = 
					serializer.deserializeInto(serializedSession.get(), 
				(RedisSession) createEmptySession());
			
			Enumeration<?> copyNames = initialSession.getAttributeNames();
			while (copyNames.hasMoreElements()) {
				String name = (String) copyNames.nextElement();
				if (ignorePropertyList.contains(name)) {
					continue;
				}
				
				Object newValue = redisSession.getAttribute(name);
				Object oldValue = initialSession.getAttribute(name);
				if (!oldValue.equals(newValue)) {
					log.info(String.format(
						"Attribute %s changed by reference, session is dirty", name));
					return true;
				}
			}
		} catch (Exception e) {
			log.warning(e.getMessage());
		}
		return false;
	}

	private void initializeDatabaseConnection() throws LifecycleException {
		try {
			connectionPool = new JedisPool(
					getJedisConfig(), getHost(), 
				getPort(), getTimeout(), getPassword());
		} catch (Exception e) {
			e.printStackTrace();
			throw new LifecycleException("Error Connecting to Redis", e);
		}
	}

	private void initializeSerializer() 
			throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		
		log.fine("Attempting to use serializer :" + serializationStrategyClass);
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

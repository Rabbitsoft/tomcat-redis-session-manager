package com.radiadesign.catalina.session;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.Valve;
import org.apache.catalina.util.LifecycleSupport;

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
	 * The lifecycle event support for this component.
	 */
	protected LifecycleSupport lifecycle = new LifecycleSupport(this);

	public int getRejectedSessions() {
		// Essentially do nothing.
		return 0;
	}

	public void setRejectedSessions(int i) {
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

	/**
	 * Add a lifecycle event listener to this component.
	 * 
	 * @param listener The listener to add
	 */
	public void addLifecycleListener(LifecycleListener listener) {
		lifecycle.addLifecycleListener(listener);
	}

	/**
	 * Get the lifecycle listeners associated with this lifecycle. If this
	 * Lifecycle has no listeners registered, a zero-length array is returned.
	 */
	public LifecycleListener[] findLifecycleListeners() {
		return lifecycle.findLifecycleListeners();
	}

	/**
	 * Remove a lifecycle event listener from this component.
	 * 
	 * @param listener The listener to remove
	 */
	public void removeLifecycleListener(LifecycleListener listener) {
		lifecycle.removeLifecycleListener(listener);
	}

	@Override
	public void start() throws LifecycleException {
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

		lifecycle.fireLifecycleEvent(START_EVENT, null);
	}

	@Override
	public void stop() throws LifecycleException {
		try {
			connectionPool.destroy();
		} catch (Exception e) {
			// Do nothing.
		}
		lifecycle.fireLifecycleEvent(STOP_EVENT, null);
	}

	@Override
	public Session createSession() {
		RedisSession session = (RedisSession) createEmptySession();

		// Initialize the properties of the new session and return it
		session.setNew(true);
		session.setValid(true);
		session.setCreationTime(System.currentTimeMillis());
		session.setMaxInactiveInterval(getMaxInactiveInterval());

		String sessionId;
		String jvmRoute = getJvmRoute();

		Boolean error = true;
		Jedis jedis = null;

		try {
			jedis = acquireConnection();

			// Ensure generation of a unique session identifier.
			do {
				sessionId = generateSessionId();
				if (jvmRoute != null) {
					sessionId += '.' + jvmRoute;
				}
			} while (jedis.setnx(sessionId.getBytes(), NULL_SESSION) == 1L); 

			/*
			 * Even though the key is set in Redis, we are not going to flag the
			 * current thread as having had the session persisted since the
			 * session isn't actually serialized to Redis yet. This ensures that
			 * the save(session) at the end of the request will serialize the
			 * session into Redis with 'set' instead of 'setnx'.
			 */
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
			byte[] binaryId = redisSession.getId().getBytes();

			jedis = acquireConnection();
			if (sessionIsDirty || currentSessionIsPersisted.get() != true || isDirtyByReference(redisSession)) {
				jedis.set(binaryId, serializer.serializeFrom(redisSession));
			}

			currentSessionIsPersisted.set(true);
			log.fine("Setting expire timeout on session [" 
				+ redisSession.getId() + "] to " + getMaxInactiveInterval());
			
			jedis.expire(binaryId, getMaxInactiveInterval());
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

	public void remove(Session session) {
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

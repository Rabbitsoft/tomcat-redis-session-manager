package com.radiadesign.catalina.session;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

public class RedisSession extends StandardSession {
	private static final long serialVersionUID = -7725512151082256670L;

	protected HashMap<String, Object> changedAttributes;
	protected Boolean dirty;

	public RedisSession(Manager manager) {
		super(manager);
		resetDirtyTracking();
	}

	public Boolean isDirty() {
		return dirty || !changedAttributes.isEmpty();
	}

	public HashMap<String, Object> getChangedAttributes() {
		return changedAttributes;
	}

	public void resetDirtyTracking() {
		changedAttributes = new HashMap<>();
		dirty = false;
	}

	@Override
	public void setAttribute(String key, Object value) {

		Object oldValue = getAttribute(key);
		super.setAttribute(key, value);

		if ((value != null || oldValue != null) && 
				(value == null && oldValue != null 
					|| oldValue == null && value != null 
					|| !value.getClass().isInstance(oldValue) || !value.equals(oldValue))) {
				changedAttributes.put(key, value);
		}
	}

	@Override
	public void removeAttribute(String name) {
		super.removeAttribute(name);
		dirty = true;
	}

	@Override
	public void setId(String id) {
		// Specifically do not call super(): it's implementation does unexpected
		// things
		// like calling manager.remove(session.id) and manager.add(session).

		this.id = id;
	}

	@Override
	public void setPrincipal(Principal principal) {
		dirty = true;
		super.setPrincipal(principal);
	}

	@Override
	public void writeObjectData(java.io.ObjectOutputStream out) throws IOException {
		super.writeObjectData(out);
		out.writeLong(this.getCreationTime());
	}

	@Override
	public void readObjectData(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		super.readObjectData(in);
		this.setCreationTime(in.readLong());
	}
}

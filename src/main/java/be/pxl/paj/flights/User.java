package be.pxl.paj.flights;

public class User {

	/**
	 * Stores the ID of the authenticated user.
	 */
	private final int id;

	/**
	 * Stores the handle of the authenticated user.
	 */
	private final String handle;

	/**
	 * Stores the full name of the authenticated user.
	 */
	private final String fullName;

	/**
	 * Creates a User with the given properties.
	 */
	public User(int id, String handle, String fullName) {
		this.id = id;
		this.handle = handle;
		this.fullName = fullName;
	}

	public int getId() {
		return id;
	}

	public String getFullName() {
		return fullName;
	}
}

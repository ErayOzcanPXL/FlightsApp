package be.pxl.paj.flights;

import java.time.LocalDate;

/**
 * Records information about a flight in the database.
 */
public class Flight {

	/**
	 * Stores the unique ID of this flight.
	 */
	private final int id;

	/**
	 * Stores the date of the flight.
	 */
	private final LocalDate date;

	/**
	 * Stores the full name of the carrier (e.g., Alaska Airlines Inc.).
	 */
	private final String carrier;

	/**
	 * Stores the flight number (which is unique only within the carrier).
	 */
	private final String flightNum;

	/**
	 * Stores the name of the city from which the flight originates.
	 */
	private final String originCity;

	/**
	 * Stores the name of the city at which the flight terminates.
	 */
	private final String destCity;

	/**
	 * Stores the flight time in minutes.
	 */
	private final int timeMinutes;

	/**
	 * Creates a flight with the given properties.
	 */
	public Flight(
			int id, LocalDate date, String carrier,
			String flightNum, String originCity, String destCity, int timeMinutes) {
		this.id = id;
		this.date = date;
		this.carrier = carrier.trim();
		this.flightNum = flightNum.trim();
		this.originCity = originCity.trim();
		this.destCity = destCity.trim();
		this.timeMinutes = timeMinutes;
	}

	public int getId() {
		return id;
	}

	public String getOriginCity() {
		return originCity;
	}

	public String getOriginCityShort() {
		return shortenCity(originCity);
	}

	public String getDestCity() {
		return destCity;
	}

	public String getDestCityShort() {
		return shortenCity(destCity);
	}

	/**
	 * Reduces a city name to at most 12 characters (truncating if necessary).
	 */
	private static String shortenCity(String cityName) {
		int index = cityName.lastIndexOf(' ');
		if (index > 0) {
			cityName = cityName.substring(0, index);
		}
		return cityName.length() <= 12 ? cityName : cityName.substring(0, 12);
	}

	public String getFlightDetails() {
		return String.format("%-12s %-12s %s %s", getOriginCityShort(), getDestCityShort(), carrier, flightNum);
	}

	public int getTimeMinutes() {
		return timeMinutes;
	}

	public LocalDate getDate() {
		return date;
	}
}

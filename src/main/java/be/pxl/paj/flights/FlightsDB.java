package be.pxl.paj.flights;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Allows clients to query and update the database in order to log in, search
 * for flights, reserve seats, show reservations, and cancel reservations.
 */
public class FlightsDB {
    public static final String QUERY_CUSTOMER_PREP_STATEMENT =
        "SELECT * FROM CUSTOMER WHERE handle = ? AND password = ?";
    public static final String QUERY_FLIGHTS_DIRECT_RESULTS_PREP_STATEMENT =
        "SELECT fid, name, flight_num, origin_city, dest_city, actual_time " +
        "FROM FLIGHTS F1, CARRIERS " +
        "WHERE carrier_id = cid AND actual_time IS NOT NULL AND " +
        "    year = ? AND month_id = ? AND day_of_month = ? AND " +
        "    origin_city = ? AND dest_city = ? " +
        "ORDER BY actual_time ASC LIMIT 99";
    public static final String QUERY_FLIGHTS_TWO_HOP_PREP_STATEMENT =
        "SELECT F1.fid as fid1, C1.name as name1, " +
        "    F1.flight_num as flight_num1, F1.origin_city as origin_city1, " +
        "    F1.dest_city as dest_city1, F1.actual_time as actual_time1, " +
        "    F2.fid as fid2, C2.name as name2, " +
        "    F2.flight_num as flight_num2, F2.origin_city as origin_city2, " +
        "    F2.dest_city as dest_city2, F2.actual_time as actual_time2 " +
        "FROM FLIGHTS F1, FLIGHTS F2, CARRIERS C1, CARRIERS C2 " +
        "WHERE F1.carrier_id = C1.cid AND F1.actual_time IS NOT NULL AND " +
        "    F2.carrier_id = C2.cid AND F2.actual_time IS NOT NULL AND " +
        "    F1.year = ? AND F1.month_id = ? AND F1.day_of_month = ? AND " +
        "    F2.year = ? AND F2.month_id = ? AND F2.day_of_month = ? AND " +
        "    F1.origin_city = ? AND F2.dest_city = ? AND" +
        "    F1.dest_city = F2.origin_city " +
        "ORDER BY F1.actual_time + F2.actual_time ASC LIMIT 99";

    private PreparedStatement queryCustomer;
    private PreparedStatement queryFlightsDirectResults;
    private PreparedStatement queryFlightsTwoHop;

    /**
     * Maximum number of reservations to allow on one flight.
     */
    private static int MAX_FLIGHT_BOOKINGS = 3;

    /**
     * Holds the connection to the database.
     */
    private Connection conn;

    /**
     * Opens a connection to the database using the given settings.
     */
    public void open(Properties settings) throws Exception {
    	// Make sure the JDBC driver is loaded.
    	// Open a connection to our database.
    	conn = DriverManager.getConnection(
    			settings.getProperty("flightservice.url"),
    			settings.getProperty("flightservice.username"),
    			settings.getProperty("flightservice.password"));
    }

    /**
     * Closes the connection to the database.
     */
    public void close() throws SQLException {
    	conn.close();
    	conn = null;
    }

    /**
     * Performs additional preparation after the connection is opened.
     */
    public void init() throws SQLException {
        queryCustomer = conn.prepareStatement(QUERY_CUSTOMER_PREP_STATEMENT);
        queryFlightsDirectResults =
            conn.prepareStatement(QUERY_FLIGHTS_DIRECT_RESULTS_PREP_STATEMENT);
        queryFlightsTwoHop = conn.prepareStatement(QUERY_FLIGHTS_TWO_HOP_PREP_STATEMENT);
    }

    /**
     * Tries to log in as the given user.
     *
     * @return The authenticated user or null if login failed.
     */
    public User logIn(String handle, String password) throws SQLException {
        queryCustomer.setString(1, handle);
        queryCustomer.setString(2, password);

        ResultSet result = queryCustomer.executeQuery();
        User user = null;
        if (result.next()) user = new User(result.getInt("uid"),
                result.getString("handle"), result.getString("name"));

        result.close();
        return user;
    }

    /**
     * Returns the list of all flights between the given cities on the given day.
     */
    public List<Flight[]> getFlights(LocalDate date, String originCity, String destCity) throws SQLException {
        queryFlightsDirectResults.setInt(1, date.getYear());
        queryFlightsDirectResults.setInt(2, date.getMonthValue());
        queryFlightsDirectResults.setInt(3, date.getDayOfMonth());
        queryFlightsDirectResults.setString(4, originCity);
        queryFlightsDirectResults.setString(5, destCity);

        ResultSet directResults = queryFlightsDirectResults.executeQuery();

        List<Flight[]> results = new ArrayList<>();
    	while (directResults.next()) {
    		results.add(new Flight[] {
    				new Flight(directResults.getInt("fid"), date,
    						directResults.getString("name"),
    						directResults.getString("flight_num"),
    						directResults.getString("origin_city"),
    						directResults.getString("dest_city"),
    						(int) directResults.getFloat("actual_time"))
    		});
    	}

    	directResults.close();

//      queryFlightsTwoHop.setInt(1, date.getYear());
//      queryFlightsTwoHop.setInt(2, date.getMonthValue());
//      queryFlightsTwoHop.setInt(3, date.getDayOfMonth());
//      queryFlightsTwoHop.setInt(4, date.getYear());
//      queryFlightsTwoHop.setInt(5, date.getMonthValue());
//      queryFlightsTwoHop.setInt(6, date.getDayOfMonth());
//      queryFlightsTwoHop.setString(7, originCity);
//      queryFlightsTwoHop.setString(8, destCity);

//  	ResultSet twoHopResults = queryFlightsTwoHop.executeQuery();
//      while (twoHopResults.next()) {
//          results.add(new Flight[] {
//                  new Flight(twoHopResults.getInt("fid1"), date,
//                          twoHopResults.getString("name1"),
//                          twoHopResults.getString("flight_num1"),
//                          twoHopResults.getString("origin_city1"),
//                          twoHopResults.getString("dest_city1"),
//                          (int) twoHopResults.getFloat("actual_time1")),
//                  new Flight(twoHopResults.getInt("fid2"), date,
//                          twoHopResults.getString("name2"),
//                          twoHopResults.getString("flight_num2"),
//                          twoHopResults.getString("origin_city2"),
//                          twoHopResults.getString("dest_city2"),
//                          (int) twoHopResults.getFloat("actual_time2"))
//          });
//      }

//      twoHopResults.close();
    	return results;
    }

    /**
     * Returns the list of all flights reserved by the given user.
     */
    public List<Flight> getReservations(User user) throws SQLException {
    	// TODO: implement this properly
    	return new ArrayList<>();
    }

    /**
     * Indicates that a reservation was added successfully.
     */
    public static final int RESERVATION_ADDED = 1;

    /**
     * Indicates the reservation could not be made because the flight is full
     * (i.e., 3 users have already booked).
     */
    public static final int RESERVATION_FLIGHT_FULL = 2;

    /**
     * Indicates the reservation could not be made because the user already has a
     * reservation on that day.
     */
    public static final int RESERVATION_DAY_FULL = 3;

    /**
     * Attempts to add a reservation for the given user on the given flights, all
     * occurring on the given day.
     *
     * @return One of the {@code RESERVATION_*} codes above.
     */
    public int addReservations(User user, LocalDate date, List<Flight> flights)
    		throws SQLException {

    	// TODO: implement this in a transaction

    	return RESERVATION_FLIGHT_FULL;
    }

    /**
     * Cancels all reservations for the given user on the given flights.
     */
    public void removeReservations(User user, List<Flight> flights)
    		throws SQLException {

    	// TODO: implement this in a transaction

    }
}

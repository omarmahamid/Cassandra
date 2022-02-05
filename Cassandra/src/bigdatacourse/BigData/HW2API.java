package bigdatacourse.hw2;

public interface HW2API {

	// connects to AstraDB
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace);
	
	// close the connection to AstraDB
	public void close();
	
	// create database tables;
	public void createTables();
	
	// initialize the prepared statements 
	public void initialize();
	
	// load-movies - loads the movies in the file into the db
	public void loadMovies(String pathMoviesFile) throws Exception;
	
	// load-users - load the users in the file into the db
	public void loadUsers(String pathUsersFile) throws Exception;
	
	// load-ratings - loads the ratings into the db
	public void loadRatings(String pathRatingsFile) throws Exception;
	
	// prints the movie's details  
	public void movie(long movieID);

	// prints the user's details
	public void user(long userID);

	// prints the user's ratings in a descending order (latest rating is printed first)
	public void userRatings(long userID);
	
	// prints the movies's ratings in a descending order (latest rating is printed first)
	public void movieRatings(long movieID);
}

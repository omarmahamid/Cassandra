package bigdatacourse.hw2.studentcode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.datastax.oss.driver.api.core.CqlSession;

import bigdatacourse.hw2.HW2API;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;


public class HW2StudentAnswer implements HW2API{

	//session
	private CqlSession session;


	//preparedStatment
	private PreparedStatement pstmtInsertMovie;
	private PreparedStatement pstmtInsertUser;
	private PreparedStatement pstmtInsertRatings;


	//VCB
	private static final String SPLIT = "::";
	private static final int ID = 0;
	private static final int MOVIE_TITLE = 1;
	private static final int MOVIE_GENRES = 2;
	private static final int USER_GENDER = 1;
	private static final int USER_AGE = 2;
	private static final int USER_OCCUPATION = 3;
	private static final int USER_ZIP_CODE = 4;
	private static final int MOVIE_ID = 1;
	private static final int RATING_ID = 2;
	private static final int DATE = 3;
	private static final String MOVIE = "MOVIE";
	private static final String USER = "USER";
	private static final String RATING = "RATING";
	private static final String RATING_1 = "RATING1";
	private static final String RATING_2 = "RATING2";
	private static final String CREATE_TABLE = "CREATE TABLE ";
	private static final String CREATED_TABLE = "created table ";
	private static final String INSERT_INTO = "INSERT INTO ";
	private static final String TABLE_MOVIE = "movie";
	private static final String TABLE_USERS = "users";
	private static final String TABLE_RATINGS = "ratings";
	private static final String CASSANDRA_CONNECTION = "please make sure you are connected to cassandra!";
	private static final String INITIAL_THE_PROJECT = "please make sure that you initialized the project";


	//CQL
	private static final String CQL_SELECT_MOVIE_QUERY =
			"SELECT * FROM " + TABLE_MOVIE + " WHERE movie_id = ?";

	private static final String CQL_SELECT_USER_QUERY =
			"SELECT * FROM " + TABLE_USERS + " WHERE user_id = ?";

	private static final String CQL_SELECT_RATING_USER =
			"SELECT * FROM " + TABLE_RATINGS + " WHERE user_id = ?";

	private static final String CQL_SELECT_RATING_MOVIE =
			"SELECT * FROM " + TABLE_RATINGS + " WHERE movie_id = ?";

	private static final String		CQL_CREATE_TABLE_MOVIE =
			CREATE_TABLE + TABLE_MOVIE 	+"(" 		+
					"movie_id bigint,"			+
					"title text,"				+
					"genres text,"	            +
					"PRIMARY KEY (movie_id)"	+
					") ";

	private static final String		CQL_CREATE_TABLE_RATING =
			CREATE_TABLE + TABLE_RATINGS 	+"(" 		+
					"id bigint, "			+
					"user_id bigint, "               +
					"movie_id bigint, "				+
					"rating bigint, "	            +
					"date bigint,"           +
					"PRIMARY KEY (id)"	+
					") ";

	private static final String		CQL_CREATE_TABLE_USERS =
			CREATE_TABLE + TABLE_USERS 	+"(" 		+
					"user_id bigint,"			+
					"gender text,"				+
					"age bigint,"	            +
					"occupation bigint, "         +
					"zip_code text, "             +
					"PRIMARY KEY (user_id)"	+
					") ";

	private static final String		CQL_MOVIE_INSERT =
			INSERT_INTO + TABLE_MOVIE + "(movie_id, title, genres) VALUES(?, ?, ?)";

	private static final String		CQL_USERS_INSERT =
			INSERT_INTO + TABLE_USERS + "(user_id, gender, age, occupation, zip_code) VALUES(?, ?, ?, ?, ?)";

	private static final String		CQL_RATINGS_INSERT =
			INSERT_INTO + TABLE_RATINGS + "(id,user_id, movie_id, rating, date) VALUES(?, ? , ?, ?, ?)";

	String createUserIndex = "CREATE INDEX idx on ratings (user_id)";
	String createMovieIndex = "CREATE INDEX idx2 on ratings (movie_id)";

	private boolean initialized = false;

	private boolean indexing = false;


	private void print(String sentence){
		System.out.println(sentence);
	}

	
	@Override
	public void connect(String pathAstraDBBundleFile, String username, String password, String keyspace) {
		if (session != null) {
			print("ERROR - cassandra is already connected");
			return;
		}
		print("Initializing connection to Cassandra...");
		this.session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get(pathAstraDBBundleFile))
				.withAuthCredentials(username, password)
				.withKeyspace(keyspace)
				.build();
		print("Initializing connection to Cassandra... Done");
	}


	@Override
	public void close() {
		if (session == null) {
			print("Cassandra connection is already closed");
			return;
		}
		print("Closing Cassandra connection...");
		session.close();
		print("Closing Cassandra connection... Done");
	}

	
	
	@Override
	public void createTables() {

		boolean createMovie = createTablesForMovies();

		boolean createUsers = createTablesForUsers();

		boolean createRating = createTablesForRatings();

		if (!createMovie || !createUsers || !createRating){
			print(CASSANDRA_CONNECTION);
			return;
		}
		print("Tables Created ...");
	}

	private boolean createTablesForMovies(){

		if (!verifyCassandraConnection()) {
			return false;
		}
		try {
			session.execute(CQL_CREATE_TABLE_MOVIE);
			print(CREATED_TABLE + TABLE_MOVIE);
		}
		catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private boolean createTablesForRatings(){
		if (!verifyCassandraConnection()) {
			return false;
		}
		try {
			session.execute(CQL_CREATE_TABLE_RATING);
			indexing = true;
			print(CREATED_TABLE + TABLE_RATINGS);
		}
		catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private boolean createTablesForUsers(){
		if(!verifyCassandraConnection()){
			return false;
		}
		try {
			session.execute(CQL_CREATE_TABLE_USERS);
			print(CREATED_TABLE + TABLE_USERS);
		}
		catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public void initialize() {

		if (!verifyCassandraConnection()){
			print(CASSANDRA_CONNECTION);
			return;
		}

		pstmtInsertMovie = session.prepare(CQL_MOVIE_INSERT);
		pstmtInsertUser = session.prepare(CQL_USERS_INSERT);
		pstmtInsertRatings = session.prepare(CQL_RATINGS_INSERT);
		initialized = true;
		print("prepared statements initialized ...");
	}


	@Override
	public void loadMovies(String pathMoviesFile) throws Exception {
		boolean loaded = loadFiles(pathMoviesFile,MOVIE);
		if (!loaded){
			return;
		}
		print("loaded movies into DB successfully.");

	}


	@Override
	public void loadUsers(String pathUsersFile) throws Exception {
		boolean loaded = loadFiles(pathUsersFile,USER);
		if (!loaded){
			return;
		}
		print("loaded users into DB successfully.");

	}


	@Override
	public void loadRatings(String pathRatingsFile) throws Exception {
		boolean loaded = loadFiles(pathRatingsFile,RATING);
		if (!loaded){
			return;
		}
		print("loaded rating into DB successfully.");
	}

	private boolean loadFiles(String path, String objectName) throws Exception{
		if (!initialized){
			print(INITIAL_THE_PROJECT);
			return false;
		}
		File file = new File(path);
		if(!verifyCassandraConnection() || !verifyFileExistence(file)){
			print(CASSANDRA_CONNECTION);
			return false;
		}

		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String rowData;
		int count = 0;
		int maxThreads = 250;
		ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);
		while ((rowData = bufferedReader.readLine()) != null){
			count++;
			String [] splitRowData = rowData.split(SPLIT);
			Object entity = createEntity(objectName,splitRowData,count);
			executorService.execute(new Runnable() {
				@Override
				public void run() {
					if (objectName.equals(MOVIE)) {
						insertEntityToDB(pstmtInsertMovie, entity, false);
					}
					if(objectName.equals(USER)){
						insertEntityToDB(pstmtInsertUser,entity,false);
					}
					if (objectName.equals(RATING)){
						insertEntityToDB(pstmtInsertRatings,entity,false);
					}
				}
			});
		}
		executorService.shutdown();
		executorService.awaitTermination(1, TimeUnit.HOURS);
		print(String.valueOf(count));
		bufferedReader.close();
		return true;
	}

	private boolean verifyCassandraConnection(){

		if(session == null){
			return false;
		}
		return true;
	}

	private boolean verifyFileExistence(File file){

		if(file == null || !file.exists()){
			print("error in the file, check the path");
			return false;
		}
		return true;
	}

	private void insertEntityToDB(PreparedStatement stmt, Object entity, boolean async){

		BoundStatement btsmt = null;
		if(entity instanceof Movie){
			btsmt = stmt.bind()
					.setLong(0,((Movie)entity).getMovieId())
					.setString(1,((Movie)entity).getTitle())
					.setString(2,((Movie)entity).getGenres());

		}else if(entity instanceof User){
			btsmt = stmt.bind()
					.setLong(0,((User)entity).getUserId())
					.setString(1,((User)entity).getGender())
					.setLong(2,((User)entity).getAge())
					.setLong(3,((User)entity).getOccupation())
					.setString(4,((User)entity).getZipCode());
		}else if (entity instanceof Rating){
			btsmt = stmt.bind()
					.setLong(0, ((Rating) entity).getCount())
					.setLong(1,((Rating) entity).getUserId())
					.setLong(2,((Rating) entity).getMovieId())
					.setLong(3,((Rating) entity).getRating())
					.setLong(4,((Rating) entity).getDate());
		}
		if(async){
			session.executeAsync(btsmt);
		}else{
			session.execute(btsmt);
		}
	}

	private Object createEntity(String objectName, String [] data, int count){

		Object object = null;
		if (objectName.equals(MOVIE)){
			object = new Movie();
			((Movie)object).setMovieId(Integer.parseInt(data[ID]));
			((Movie)object).setTitle(data[MOVIE_TITLE]);
			((Movie)object).setGenres(data[MOVIE_GENRES]);
		}

		if (objectName.equals(USER)){
			object = new User();
			((User)object).setUserId(Integer.parseInt(data[ID]));
			((User)object).setAge(Integer.parseInt(data[USER_AGE]));
			((User)object).setGender(data[USER_GENDER]);
			((User)object).setOccupation(Long.parseLong(data[USER_OCCUPATION]));
			((User)object).setZipCode(data[USER_ZIP_CODE]);
		}
		if (objectName.equals(RATING)){
			object = new Rating();
			((Rating) object).setCount(count);
			((Rating) object).setUserId(Integer.parseInt(data[ID]));
			((Rating) object).setMovieId(Integer.parseInt(data[MOVIE_ID]));
			((Rating) object).setRating(Integer.parseInt(data[RATING_ID]));
			((Rating) object).setDate(Long.parseLong(data[DATE]));
		}
		return object;
	}


	@Override
	public void movie(long movieID) {
		executeSelectQuery(movieID,MOVIE);
	}

	@Override
	public void user(long userID) {
		executeSelectQuery(userID,USER);
	}

	@Override
	public void userRatings(long userID) {
		if (indexing) {
			createIndexForRatings(USER);
		}
		executeSelectQuery(userID,RATING_1);
	}

	@Override
	public void movieRatings(long movieID) {
		if (indexing) {
			createIndexForRatings(MOVIE);
		}
		executeSelectQuery(movieID,RATING_2);
	}

	private void createIndexForRatings(String type){
		if (type.equals(USER)){
			session.execute(createUserIndex);
		}
		if (type.equals(MOVIE)){
			session.execute(createMovieIndex);
		}
	}


	private void executeSelectQuery(long id,String queryType){
		if(!verifyCassandraConnection()){
			print(CASSANDRA_CONNECTION);
			return;
		}
		if(!initialized){
			print(INITIAL_THE_PROJECT);
			return;
		}
		ResultSet rs = null;
		switch (queryType){

			case MOVIE:
				rs = session.execute(CQL_SELECT_MOVIE_QUERY,id);
				break;

			case USER:
				rs = session.execute(CQL_SELECT_USER_QUERY,id);
				break;

			case RATING_1:
				rs = session.execute(CQL_SELECT_RATING_USER,id);
				break;
			case RATING_2:
				rs = session.execute(CQL_SELECT_RATING_MOVIE,id);
				break;
		}
		printAsDesiredFormat(queryType,rs);
	}

	private void printAsDesiredFormat(String queryType, ResultSet rs){
		switch (queryType){
			case MOVIE:
				printMovieFormat(rs);
				break;
			case USER:
				printUserFormat(rs);
				break;
			case RATING_1:
				printRatingFormat(rs,MOVIE);
				break;
			case RATING_2:
				printRatingFormat(rs,USER);
				break;
		}
	}

	private void printMovieFormat(ResultSet rs){
		if (rs == null){
			return;
		}
		Row row = rs.one();
		while (row != null) {
			print("id: " + row.getLong(0));
			print("genres: " + row.getString(1));
			print("title: " + row.getString(2));
			row = rs.one();
		}
	}

	private void printUserFormat(ResultSet rs){
		if(rs == null){
			return;
		}
		Row row = rs.one();
		while (row != null) {
			print("id: " + row.getLong(0) + " age: " + row.getLong(1) + " gender: " + row.getString(2) + " occupation: " + row
			.getLong(3) + " zip_code: " + row.getString(4));
			row = rs.one();
		}
	}
	private void printRatingFormat(ResultSet rs, String typeOfQuery){
		if (rs == null){
			return;
		}
		int index = typeOfQuery.equals(MOVIE) ? 2 : 4;
		int count = 0;
		Row row = rs.one();
		while (row != null) {
			print("date: " + Instant.ofEpochMilli(row.getLong(1)));
			print(typeOfQuery.toLowerCase() + ": " + row.getLong(index));
			print("rating: " + row.getLong(3));
			row = rs.one();
			count++;
		}
		print("total rating: " + count);
	}

}

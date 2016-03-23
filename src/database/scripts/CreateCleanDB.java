package database.scripts;

import database.MySQL_UI;

/**
 * Quick script that creates a brand new database. Requires three command line arguments, host port user, in that
 * order. A fourth optional parameter is password. If none is provided, the default sql password, the empty string,
 * is user.
 *
 * @author Chris Moghbel
 */
public class CreateCleanDB {

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Must provide 3 arguments: host port user");
      System.exit(1);
    }

    String password = "";
    if (args.length == 3) {
      System.out.println("No password provided, using default password.");
    }
    else {
      password = args[4];
    }

    System.out.println("Attempting to set up brand new database.");

    MySQL_UI sql = new MySQL_UI(args[0], args[1], args[2], password);
    sql.setupDatabase();

    System.out.println("Successfully created brand new database.");
  }

}

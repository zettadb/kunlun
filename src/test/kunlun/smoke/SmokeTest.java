package kunlun.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class SmokeTest {

    static {
        try {
            Class.forName("org.postgresql.Driver");
            //Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception ex) {
        }
    }

    public static Connection getConnection(String user,
                                           String password,
                                           String host,
                                           int port,
                                           String dbname) {
        //String proto = "postgres";
        String proto = "postgresql";
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        String url = "jdbc:" + proto+"://" + host + ":" + port + "/" + dbname;
        try {
            return DriverManager.getConnection(url, props);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    /*
     * We do the following actions:
     * 1 Create the able
     * 2 Insert two records
     * 3 Update the first record.
     * 4 Query the records(res1).
     * 5 Delete the second record.
     * 6 Query the records again(res2).
     * 7 Drop the table.
     */
    public static void smokeTest(Connection conn) throws Exception{
        boolean autocommit = conn.getAutoCommit();
        System.out.println("default autocommit: " + autocommit);
        conn.setAutoCommit(true);
        Statement st =conn.createStatement();
        st.execute("drop table if exists t1;");
        String createSql = "create table t1(id integer primary key, " +
                           "info text, wt integer);";
        st.execute(createSql);
        st.execute("insert into t1(id,info,wt) values(1, 'record1', 1);");
        st.execute("insert into t1(id,info,wt) values(2, 'record2', 2);");
        st.execute("update t1 set wt = 12 where id = 1;");
        ResultSet res1 = st.executeQuery("select * from t1;");
        System.out.printf("res1:%s%n", showResults(res1).toString());
        st.execute("delete from t1 where id = 1;");
        ResultSet res2 = st.executeQuery("select * from t1;");
        System.out.printf("res2:%s%n", showResults(res2).toString());
		//"prepare q1(int) as select*from t1111 where id=$1","execute q1(1)","execute q1(2)",
		//"prepare q2(text,int, int) as update t1111 set info=$1 , wt=$2 where id=$3", "execute q2('Rec1',2,1)", "execute q2('Rec2',3,2)",
		PreparedStatement pstmt0 = conn.prepareStatement("insert into t1 values(?, ?, ?)");

		conn.setAutoCommit(false);
		pstmt0.setInt(1,1);
		pstmt0.setString(2, "rec1");
		pstmt0.setInt(3,11);
		pstmt0.executeUpdate();
		conn.commit();


        conn.setAutoCommit(true);
		PreparedStatement pstmt1 = conn.prepareStatement("select*from t1 where id=?");
		pstmt1.setInt(1, 1);
		ResultSet rs1 = pstmt1.executeQuery();
        System.out.printf("pstmt1.rs1:%s%n", showResults(rs1).toString());
		rs1.close();

		conn.setAutoCommit(false);
		pstmt1.setInt(1, 2);
		rs1 = pstmt1.executeQuery();
        System.out.printf("pstmt1.rs1:%s%n", showResults(rs1).toString());
		rs1.close();

		PreparedStatement pstmt2 = conn.prepareStatement("update t1 set info=? , wt=? where id=?");
		pstmt2.setString(1, "Rec1");
		pstmt2.setInt(2,2);
		pstmt2.setInt(3,1);
		pstmt2.executeUpdate();
		conn.commit();

        conn.setAutoCommit(true);
		pstmt2.setString(1, "Rec2");
		pstmt2.setInt(2,3);
		pstmt2.setInt(3,2);
		pstmt2.executeUpdate();

		PreparedStatement pstmt3 = conn.prepareStatement("select*from t1 where id = ?");
		pstmt3.setInt(1,1);
		ResultSet rs3 = pstmt3.executeQuery();
        System.out.printf("pstmt3.rs3:%s%n", showResults(rs3).toString());
		rs3.close();

		pstmt3.setInt(1,2);
		rs3 = pstmt3.executeQuery();
        System.out.printf("pstmt3.rs3:%s%n", showResults(rs3).toString());
		rs3.close();

        st.execute("drop table t1;");
        conn.setAutoCommit(autocommit);
    }

    private static List<List<String>> showResults(ResultSet res)
        throws Exception {
        LinkedList<List<String>> results = new LinkedList<>();
        int cols = res.getMetaData().getColumnCount();
        while (res.next()) {
            List<String> row = new ArrayList<>(cols);
            for (int i = 0; i < cols; i++) {
                row.add(res.getString(i + 1));
            }
            results.addLast(row);
        }
        return results;
    }

    public static void test1() throws Exception{
        String host = "127.0.0.1";
        int port = 5404;
        String user = "abc";
        String password = "abc";
        String database = "postgres";
        Connection conn = getConnection(user, password, host, port, database);
        smokeTest(conn);
    }

    public static void test2() throws Exception {
        String host = "127.0.0.1";
        int port = 5404;
        String user = "abc";
        String password = "abc";
        String database = "postgres";
        Connection conn = getConnection(user, password, host, port, database);
        smokeTest(conn);
    }

    public static void main(String[] args) throws Exception {
        test1();
        // test2();
    }

}

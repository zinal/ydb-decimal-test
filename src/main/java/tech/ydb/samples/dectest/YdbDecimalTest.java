package tech.ydb.samples.dectest;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 * @author mzinal
 */
public class YdbDecimalTest implements AutoCloseable {

    private Connection connection = null;

    public static void main(String[] args) {
        try (YdbDecimalTest instance = new YdbDecimalTest()) {
            instance.init();
            instance.run();
        } catch(Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

    public void run() throws Exception {
        makeTable();
        insertRows();
        selectRows();
        aggregateRows();
        filterRows();
    }

    public void init() throws Exception {
        String connString = System.getenv("YDB_CONNECTION_STRING");
        if (connString==null) {
            connString = "grpc://localhost:2135/local";
        }
        String jdbcUrl = "jdbc:ydb:" + connString + "?useQueryService=true";
        String tlsFile = System.getenv("YDB_SSL_ROOT_CERTIFICATES_FILE");
        if (tlsFile!=null) {
            jdbcUrl = jdbcUrl + "&secureConnectionCertificate=file:" + tlsFile;
        }
        String login = System.getenv("YDB_USER");
        if (login==null) {
            String saFile = System.getenv("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS");
            if (saFile!=null) {
                jdbcUrl = jdbcUrl + "&saFile=file:" + saFile;
            }
            this.connection = DriverManager.getConnection(jdbcUrl);
        } else {
            this.connection = DriverManager.getConnection(jdbcUrl, login, System.getenv("YDB_PASSWORD"));
        }
        this.connection.setAutoCommit(false);
    }

    @Override
    public void close() {
        if (connection!=null) {
            try {
                connection.close();
            } catch(Exception ex) {}
            connection = null;
        }
    }

    private void makeTable() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE dectest1(a int32, b decimal(22,9), PRIMARY KEY(a))");
        }
        System.out.println("created the dectest1 table!");
    }

    private void insertRows() throws Exception {
        System.out.println("writing...");
        String sql = "UPSERT INTO dectest1(a,b) VALUES(?, ?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, new BigDecimal("555555555555555.0"));
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setBigDecimal(2, new BigDecimal("455555555555555.0"));
            ps.addBatch();
            ps.executeBatch();
        }
        connection.commit();
        System.out.println("committed!");
    }

    private void selectRows() throws Exception {
        System.out.println("reading...");
        String sql = "SELECT a,b FROM dectest1 ORDER BY a LIMIT 100";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println("... " + rs.getBigDecimal(2).toString());
                }
            }
        }
        System.out.println("ready!");
    }

    private void aggregateRows() throws Exception {
        System.out.println("aggregating...");
        String sql = "SELECT SUM(b) FROM dectest1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println("... " + rs.getBigDecimal(1).toString());
                }
            }
        }
        System.out.println("ready!");
    }

    private void filterRows() throws Exception {
        System.out.println("filtering...");
        String sql = "SELECT b FROM dectest1 WHERE b>?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setBigDecimal(1, new BigDecimal("455555555555555.0"));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    System.out.println("... " + rs.getBigDecimal(1).toString());
                }
            }
        }
        System.out.println("ready!");
    }
}

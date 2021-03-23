package Utils;

/**
 * @author ：WSS
 * @date ：Created in 2019/9/10 10:29
 * @description：Database Connection
 * @modified By：Jesse.Qi
 * @version: 1.1$
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class DBUtils {

    //    private String connStr;
    private static String url;
    private static String UserID;
    private static String Password;

    private static DBUtils dbUtilsInstance = null;

    public static DBUtils getInstance() {
        if (dbUtilsInstance == null) {
            dbUtilsInstance = new DBUtils();
        }

        return dbUtilsInstance;
    }

    private Connection Conn() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            conn = DriverManager.getConnection(url, UserID, Password);

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return conn;
    }

    private DBUtils() {
        url = XmlUtils.ReadXml("url");
        UserID = XmlUtils.ReadXml("UserID");
        Password = XmlUtils.ReadXml("Password");
//        connStr = url + "?user=" + UserID + "&password=" + Password;
    }


    @SuppressWarnings({"unchecked", "rawtypes"})
    public ArrayList executeQuery(String strSQL) {
        ArrayList resList = null;
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);
            ResultSetMetaData md = resultSet.getMetaData();
            int num = md.getColumnCount();
            resList = new ArrayList();
            while (resultSet.next()) {
                Map mapOfColValues = new HashMap(num);
                for (int i = 1; i <= num; i++) {
                    mapOfColValues.put(md.getColumnName(i), resultSet.getObject(i));
                }
                resList.add(mapOfColValues);
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return resList;
    }

    public ArrayList executeOneLine(String strSQL) {
        ArrayList resList = null;
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);
            ResultSetMetaData md = resultSet.getMetaData();
            int num = md.getColumnCount();
            resList = new ArrayList();
            while (resultSet.next()) {
                Map mapOfColValues = new HashMap(num);
                for (int i = 1; i <= num; i++) {
                    resList.add(resultSet.getObject(i).toString());
                }
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return resList;
    }


    public int executeNonQuery(String strSQL) {
        int res = -1;
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);

            res = 0;
            while (resultSet.next()) {
                res++;
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }

    public int executeQueryScalarInt(String strSQL) {
        int res = -1;
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);

            if (resultSet.next()) {
                res = resultSet.getInt(1);
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }

    public Map<String, String> executeFillData(String strSQL) {
        Map<String, String> res = new HashMap<String, String>();
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);

            while (resultSet.next()) {
                res.put(resultSet.getString(1), resultSet.getString(2));
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;

    }

    public Map<String, String> executeFillCoorData(String strSQL) {
        Map<String, String> res = new LinkedHashMap<String, String>();
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);

            while (resultSet.next()) {
                res.put(resultSet.getString(1), resultSet.getString(2) + " " + resultSet.getString(3));
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;

    }

    public String executeQueryScalar(String strSQL) {
        String res = null;
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);

            if (resultSet.next()) {
                res = resultSet.getString(1);
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }

    public boolean execute(String strSQL) {
        boolean res = false;
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            res = statement.execute(strSQL);
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }

    public int executeUpdate(String strSQL) {
        int result = -1;
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            result = statement.executeUpdate(strSQL);
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return result;
    }

    public boolean isExistTable(String tableName) {
        boolean res = false;
        try {
            String sql = "select count(*) from pg_class where relname='"
                    + tableName + "'";
            int count = executeQueryScalarInt(sql);
            if (count > 0) {
                res = true;
            }

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }

    /**
     * ��ѯ���ݿ����Ƿ���ڸ�����
     *
     * @param tableName
     * @param colName
     * @param value
     * @return
     */
    public Boolean isExistData(String tableName, String colName, String value) {
        boolean res = false;
        try {
            String sql = "Select " + colName + " from " + tableName + " where " + colName + " = '" + value + "'";
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            if (resultSet.next()) {
                res = true;
            }
            resultSet.close();
            statement.close();
            conn.close();
        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }


    public Boolean isMatchData(String tableName, String colName, String value) {
        boolean res = false;
        try {
            String sql = "Select count(*) from " + tableName + " where " + colName + " like '%" + value + "%'";
            int count = executeQueryScalarInt(sql);
            if (count > 0) {
                res = true;
            }

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }

    public ArrayList<String> executeQueryList(String strSQL) {
        ArrayList<String> res = new ArrayList<String>();
        try {
            Connection conn = Conn();
            Statement statement = conn.createStatement();
            ResultSet resultSet = statement.executeQuery(strSQL);

            while (resultSet.next()) {
                res.add(resultSet.getString(1));
            }

            resultSet.close();
            statement.close();
            conn.close();

        } catch (Exception e) {
            LogUtils.getInstance().logException(e);
        }

        return res;
    }


}

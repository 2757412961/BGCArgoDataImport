package Utils;

/**
 * @author ：Jesse.Qi
 * @date ：Created in 2019/9/10 21:25
 * @description：Read files BaseClass
 * @modified By：Jesse.Qi
 * @version: $1.0
 */

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public abstract class ReadBase {

    public String getDataIdentity() {
        return "ReadBase";
    }

    protected ReadBase() {
    }

//    public abstract void readFile() ;

    public String getNow() {
        Date dat = new Date();
        Calendar calendar = Calendar.getInstance();
        dat = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateNowStr = sdf.format(dat);
        return dateNowStr;
    }

    public String compareTime(String timeStr) {
        String res = "false";
        Date datNow = new Date();
        Calendar calendar = Calendar.getInstance();
        datNow = calendar.getTime();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = sdf.parse(timeStr);
            long from1 = date.getTime();
            long to1 = datNow.getTime();
            int days = (int) ((to1 - from1) / (1000 * 60 * 60 * 24));
            if (days > 180) {
                res = "false";
            } else {
                res = "true";
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return res;
        }
    }

    public String ConvertCentre(String shortname) {
        String longname = "";
        switch (shortname) {
            case "AO":
                longname = "AOML";
                break;
            case "BO":
                longname = "BODC";
                break;
            case "CI":
                longname = "";
                break;
            case "CS":
                longname = "CSIRO";
                break;
            case "GE":
                longname = "BSH";
                break;
            case "GT":
                longname = "GTS";
                break;
            case "HZ":
                longname = "CSIO";
                break;
            case "IF":
                longname = "CORIOLIS";
                break;  //Ifremer???
            case "IN":
                longname = "INCOIS";
                break;
            case "JA":
                longname = "JMA";
                break;
            case "JM":
                longname = "JAMSTEC";
                break;
            case "KM":
                longname = "KMA";
                break;
            case "KO":
                longname = "KORDI";
                break;
            case "MB":
                longname = "MBARI";
                break;
            case "ME":
                longname = "MEDS";
                break;
            case "NA":
                longname = "NAVO";
                break;
            case "NM":
                longname = "NMDIS";
                break;
            case "PM":
                longname = "PMEL";
                break;
            case "RU":
                longname = "";
                break;
            case "SI":
                longname = "SIO";
                break;
            case "SP":
                longname = "";
                break;
            case "UW":
                longname = "";
                break;
            case "VL":
                longname = "";
                break;
            case "WH":
                longname = "";
                break;
            default:
                break;
        }
        return longname;
    }

    public String ConvertCountry(String shortname) {
        String country = "";
        switch (shortname) {
            case "AO":
                country = "USA";
                break;
            case "BO":
                country = "United Kingdom";
                break;
            case "CI":
                country = "Canada";
                break;
            case "CS":
                country = "Australia";
                break;
            case "GE":
                country = "Germany";
                break;
            case "GT":
                country = "WMO";
                break;//????
            case "HZ":
                country = "China";
                break;
            case "IF":
                country = "France";
                break;
            case "IN":
                country = "India";
                break;
            case "JA":
                country = "Japan";
                break;
            case "JM":
                country = "Japan";
                break;
            case "KM":
                country = "Korea";
                break;
            case "KO":
                country = "Korea";
                break;
            case "MB":
                country = "USA";
                break;
            case "ME":
                country = "Canada";
                break;
            case "NA":
                country = "USA";
                break;
            case "NM":
                country = "China";
                break;
            case "PM":
                country = "USA";
                break;
            case "RU":
                country = "Russia";
                break;
            case "SI":
                country = "USA";
                break;
            case "SP":
                country = "Spain";
                break;
            case "UW":
                country = "USA";
                break;
            case "VL":
                country = "Russia";
                break;
            case "WH":
                country = "USA";
                break;
            default:
                break;
        }
        return country;
    }

    public void lastCycle(ArrayList<String> bgcList) {
        String Sql = "";
        int maxcycle = -1;
        ArrayList<String> info = new ArrayList<String>();
        String active = "";

        for (String platform_number : bgcList) {
            Sql = " select max(cycle_number) from bgc_profile where platform_number='" + platform_number + "' ";
            //查出最大的cycle,組成profile_name
            if (DBUtils.getInstance().executeQueryScalar(Sql) == null) {
                continue;
            }
            maxcycle = Integer.parseInt(DBUtils.getInstance().executeQueryScalar(Sql));
            Sql = " select cycle_number,latitude,longitude,date from bgc_profile where platform_number='" + platform_number + "' and cycle_number=" + maxcycle;
            info = DBUtils.getInstance().executeOneLine(Sql);
            if (info.size() < 3) {
                LogUtils.getInstance().logInfo( "Updating bgc_metadata " +platform_number + " info lose" );
                continue;
            }
            String timeStr = info.get(3);
            if (timeStr.endsWith(".0")) {
                timeStr = timeStr.replace(".0", "");
            }
            active = compareTime(timeStr);
            Sql = "Update bgc_metadata set last_cycle='" + info.get(0) + "', last_latitude=" + info.get(1)
                    + ", last_longitude=" + info.get(2) + ", last_date=to_timestamp('" + timeStr
                    + "','YYYY-MM-DD HH24:MI:SS'), \"active\"=" + active + " where platform_number='" + platform_number + "' ";
            DBUtils.getInstance().executeUpdate(Sql);
            LogUtils.getInstance().logInfo("Update bgc_metadata " + platform_number + " last cycle:" + info.get(0));
        }
    }


}

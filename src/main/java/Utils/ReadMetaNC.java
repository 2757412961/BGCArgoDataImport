package Utils;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author ：Jesse.Qi
 * @date ：Created in 2019/9/10 20:36
 * @description：read metafiles(.nc)
 * @modified By：
 * @version: 1.0$
 */

public class ReadMetaNC extends ReadBase {

    private static ReadMetaNC ReadMetaNCInstance = null;

    protected String[] metaVars;

    protected static NetcdfFile ncfile = null;

    protected ReadMetaNC() {
        metaVars = new String[]{"LAUNCH_DATE", "LAUNCH_LATITUDE", "LAUNCH_LONGITUDE", "DATE_CREATION",
                "FORMAT_VERSION", "PTT", "TRANS_SYSTEM", "POSITIONING_SYSTEM", "FLOAT_SERIAL_NO", "DATA_CENTRE",
                "PROJECT_NAME", "PARAMETER", "SENSOR", "PI_NAME", "PLATFORM_MAKER", "PLATFORM_TYPE"};
    }

    public static ReadMetaNC getInstance() {
        if (ReadMetaNCInstance == null) {
            ReadMetaNCInstance = new ReadMetaNC();
        }

        return ReadMetaNCInstance;
    }

    public String getDataIdentity() {
        return "ReadMetaNC";
    }

    public void readFile(ArrayList<String> metaList) {
        String SQL = "";
        for (String metafile : metaList) {//非测试时需要这个循环
//        String metafile = "F:/BGCArgo/BgcData/2902753_meta.nc";
//        String preSQL="Insert into bgc_metadata (platform_number, institution, t_file, profiler_type, date_update)";
//        preSQL+=" VALUES ('2902753','HZ','csio/2902753/2902753_meta.nc','841',to_timestamp('20190330015530','YYYYMMDDHH24MISS'))";
//        DBUtils.getInstance().executeQuery(preSQL);
            String platform_number = metafile.substring(metafile.lastIndexOf("/") + 1, metafile.lastIndexOf("_meta"));
            String[] metaValues = readNC(metafile, metaVars);
            if (metaValues[metaValues.length - 1] == null) {
                continue;
            }
            SQL = "UPDATE bgc_metadata set time_write=to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS'),";
            for (int i = 0; i < metaVars.length; i++) {
//            for (int i = 0; i < 4; i++) {
//                LogUtils.getInstance().logInfo(metaVars[i] + " : " + metaValues[i]);
                if (metaVars[i].toLowerCase().contains("time_") || metaVars[i].toLowerCase().contains("date")) {
                    SQL += metaVars[i] + "=to_timestamp('" + metaValues[i] + "','YYYYMMDDHH24MISS'),";
                } else {
                    // 在PG 9.1及以后，普通字符串中的\不会被任务是转义字符，而E'xx\x'中的\才会被当作是转义字符。
                    // 所以sql中已经没有转移字符了，单引号报错是因为引号配对错误
                    SQL += metaVars[i] + "='" + metaValues[i].replace("'", "''") + "',";
                }
            }
//        SQL = SQL.substring(0, SQL.length() - 1) + " where platform_number='" + platform_number + "' ";
            SQL = SQL.substring(0, SQL.length() - 1)
                    + ", nc_path = '" + metafile + "'"
                    + " where platform_number='" + platform_number + "' ";
            DBUtils.getInstance().executeUpdate(SQL);
        }
    }

    //读取nc文件中的指定字段
    public String[] readNC(String filepath, String[] Vars) {
        String[] Values = new String[Vars.length];
        LogUtils.getInstance().logInfo("Opening:" + filepath);
        try {
            ncfile = NetcdfFile.open(filepath);
            for (int i = 0; i < metaVars.length; i++) {
                Variable thisV = ncfile.findVariable(metaVars[i]);
                if (thisV == null) {
                    throw new IllegalArgumentException();
                }
                if (thisV.getDimensions().size() > 1) {
                    Array temp = thisV.read();
                    Values[i] = temp.toString().replace(" ", "").replace(",", ";");
                } else {
                    if (thisV.getDataType() == DataType.DOUBLE) {
                        Values[i] = String.valueOf(thisV.readScalarDouble());
                    } else {
                        Values[i] = thisV.readScalarString().trim();
                    }
                }
            }

        } catch (IOException ioe) {
            LogUtils.getInstance().logException("trying to open " + filepath + ioe);
        } catch (IllegalArgumentException ioe2) {
            LogUtils.getInstance().logException("Lose para " + filepath + ioe2);
        } finally {
            if (null != ncfile) try {
                ncfile.close();
            } catch (IOException ioe) {
                LogUtils.getInstance().logException("trying to close " + filepath + ioe);
            }

        }

        return Values;
    }

}

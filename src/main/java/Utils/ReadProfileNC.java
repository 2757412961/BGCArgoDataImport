package Utils;

import org.json.JSONException;
import org.json.JSONObject;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ：Jesse.Qi
 * @date ：Created in 2019/9/10 21:42
 * @description：
 * @modified By：
 * @version: 1.0$
 */

public class ReadProfileNC extends ReadBase {

    private static ReadProfileNC ReadProfileNCInstance = null;

    protected String[] ProfileVars;
    protected String[] addlonlatVars;

    protected ArrayList<String> DataVars;

    protected static NetcdfFile ncfile = null;

    protected ReadProfileNC() {
        ProfileVars = new String[]{"DATE_CREATION", "JULD", "FORMAT_VERSION"};
        addlonlatVars = new String[]{"LONGITUDE", "LATITUDE"};
        DataVars = new ArrayList<String>();
    }

    public static ReadProfileNC getInstance() {
        if (ReadProfileNCInstance == null) {
            ReadProfileNCInstance = new ReadProfileNC();
        }

        return ReadProfileNCInstance;
    }

    public String getDataIdentity() {
        return "ReadProfileNC";
    }

    public void readFile(ArrayList<String> profileList) {
        String SQL = "";
        for (String proNC_path : profileList) { // F:/BGCArgo/BgcData/BGCAgro/Download/aoml/1900722/profiles/MD1900722_001.nc
//        String proNC_path = "F:/BGCArgo/BgcData/BGCAgro/Download/aoml/1900722/profiles/MD1900722_001.nc";
//            String profile_name = proNC_path.substring(proNC_path.lastIndexOf('/') + 3, proNC_path.indexOf(".nc")); //R2902753_001.nc就不行了
            String profile_name = proNC_path.substring(proNC_path.indexOf(".nc") - 11, proNC_path.indexOf(".nc"));

            String[] ProfileValues = readNC(proNC_path, ProfileVars);
            if (ProfileValues.length == ProfileVars.length) {
                SQL = "UPDATE bgc_profile set time_write=to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS'),";
                for (int i = 0; i < ProfileVars.length; i++) {
                    LogUtils.getInstance().logInfo(ProfileVars[i] + " : " + ProfileValues[i]);
                    if (ProfileVars[i].contains("time_") || ProfileVars[i].contains("date") || ProfileVars[i].contains("DATE")) {
                        SQL += ProfileVars[i] + "=to_timestamp('" + ProfileValues[i] + "','YYYYMMDDHH24MISS'),";
                    } else {
                        SQL += ProfileVars[i] + "='" + ProfileValues[i] + "',";
                    }
                }
                SQL = SQL.substring(0, SQL.length() - 1) + " where profile_name='" + profile_name + "' ";
                DBUtils.getInstance().executeUpdate(SQL);
            }
//            String platform_number = proNC_path.substring(proNC_path.lastIndexOf('/') + 3, proNC_path.indexOf("_")); //R2902753_001.nc就不行了
            String platform_number = proNC_path.substring(proNC_path.indexOf(".nc") - 11, proNC_path.indexOf("_"));
            SQL = "select parameter from bgc_metadata where platform_number='" + platform_number + "' ";
            String parasCombine = DBUtils.getInstance().executeQueryScalar(SQL);
            if (parasCombine == null) {
                continue;
            }
            /////////////////////////////////
            //开始生成json过程
            /////////////////////////////////
            ArrayList<String> paras = new ArrayList<String>();//确定参数
            ArrayList<Integer> combineType = new ArrayList<Integer>();//剖面参数号
            //int jsonNumber = 0;//json顺序号
            paras.add("P");
            DataVars.add("PRES");
            paras.add("T");
            DataVars.add("TEMP");
            combineType.add(0);
            //jsonNumber++;
            if (parasCombine.contains("PSAL")) {
                paras.add("S");
                DataVars.add("PSAL");
                //jsonNumber++;
                combineType.add(1);
            }
            if (parasCombine.contains("DOXY")) {
                paras.add("O");
                DataVars.add("DOXY");
                //jsonNumber++;
                combineType.add(2);
            }
            if (parasCombine.contains("CHLA")) {
                paras.add("C");
                DataVars.add("CHLA");
                //jsonNumber++;
                combineType.add(3);
            }
            if (parasCombine.contains("NITRATE")) {
                paras.add("N");
                DataVars.add("NITRATE");
                //jsonNumber++;
                combineType.add(4);
            }
            if (parasCombine.contains("PH_IN_SITU_TOTAL")) {
                paras.add("J");
                DataVars.add("PH_IN_SITU_TOTAL");
                //jsonNumber++;
                combineType.add(5);
            }
            if (parasCombine.contains("CDOM")) {
                paras.add("M");
                DataVars.add("CDOM");
                combineType.add(6);
                //jsonNumber++;
            }
            if (parasCombine.contains("BBP700")) {
                paras.add("B");
                DataVars.add("BBP700");
                combineType.add(7);
                //jsonNumber++;
            }
            if (parasCombine.contains("DOWN")) {
                int number = 1;
                if (parasCombine.contains("DOWN_IRRADIANCE380")) {
                    DataVars.add("DOWN_IRRADIANCE380");
                    paras.add("R" + Integer.toString(number++));
                }
                if (parasCombine.contains("DOWN_IRRADIANCE412")) {
                    DataVars.add("DOWN_IRRADIANCE412");
                    paras.add("R" + Integer.toString(number++));
                }
                if (parasCombine.contains("DOWN_IRRADIANCE443")) {
                    DataVars.add("DOWN_IRRADIANCE443");
                    paras.add("R" + Integer.toString(number++));
                }
                if (parasCombine.contains("DOWN_IRRADIANCE490")) {
                    DataVars.add("DOWN_IRRADIANCE443");
                    paras.add("R" + Integer.toString(number++));
                }
                if (parasCombine.contains("DOWNWELLING_PAR")) {
                    DataVars.add("DOWNWELLING_PAR");
                    paras.add("R" + Integer.toString(number++));
                }
//                paras.add("R1");
//                paras.add("R2");
//                paras.add("R3");
//                paras.add("R4");
                combineType.add(8);
                //jsonNumber++;
            }
//            String[] DataValues = new String[DataVars.size()];//获取数据
            ArrayList<String> DataValues = new ArrayList<>();//获取数据
            int rows = 0;//监测数据的行数
            int cols = 0;//监测数据的列数

//            LogUtils.getInstance().logInfo("Opening again:" + proNC_path);
            try {
                ncfile = NetcdfFile.open(proNC_path);
                for (int i = 0; i < DataVars.size(); i++) {
                    Variable thisV = ncfile.findVariable(DataVars.get(i));
                    if (rows == 0 || cols == 0) {
                        List<Dimension> dms = thisV.getDimensions();
                        String dm_1 = dms.get(0).toString();
                        String dm_2 = dms.get(1).toString();
                        rows = Integer.parseInt(dm_1.substring(dm_1.indexOf("=") + 1, dm_1.length() - 1).trim());
                        cols = Integer.parseInt(dm_2.substring(dm_2.indexOf("=") + 1, dm_2.length() - 1).trim());
                    }

//                    Array temp = thisV.read();
//                    DataValues[i] = temp.toString();
//                    DataValues.add(temp.toString());
                    if (thisV != null) {
                        Array temp = thisV.read();
                        DataValues.add(temp.toString());
                    } else {
                        String temp = "";
                        for (int k = 0; k < rows * cols; k++) {
                            temp += "99999.0 ";
                        }
                        DataValues.add(temp);
                    }
                }
                //获得了“数据块”，对其进行“分开”,转换为与paras对应的一行数组，其中压力得分给多个paras

                //首先是压力，必有多行
                ArrayList<Integer> usePres = new ArrayList<Integer>();//记录压力不为0的行号
                String[][] pres_allR = new String[rows][cols];//记录row*cols的压力数据
//                String[] splited_0 = DataValues[0].split(" ");
                String[] splited_0 = DataValues.get(0).split(" ");
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        pres_allR[i][j] = splited_0[i * cols + j];
                        if (j == 0) {
                            if (!pres_allR[i][0].equals("99999.0"))//注意一下这个缺省值
                            {
                                usePres.add(i);
                            }
                        }
                    }
                }

                // 判断字段数形是否全为99999.0  2019-12-31 添加
                ArrayList<Integer> removeIndex = new ArrayList<>();
                for (int pn = 1; pn < paras.size(); pn++) {
//                    String[] splited = DataValues[pn + 1].split(" ");
                    String[] splited = DataValues.get(pn).split(" ");
                    boolean flag = false;
                    for (int i = 0; i < rows; i++) {
                        if (!splited[i * cols].equals("99999.0"))//注意一下这个缺省值
                        {
                            flag = true;
                        }
                    }
                    if (flag == false) { // 数据全部为99999.0
                        removeIndex.add(pn);
                    }
                }
                for (int i = removeIndex.size() - 1; i >= 0; i--) {
                    int ri = removeIndex.get(i);
                    paras.remove(ri);
                    if (ri < combineType.size() && combineType.get(ri) != 8) combineType.remove(ri);
                    DataVars.remove(ri);
                    DataValues.remove(ri);
                }
                if (combineType.size() > paras.size())
                    combineType.remove(combineType.size() - 1);

                //然后是其他参数，目前都是只有一行的
                ArrayList<Integer> useRow = new ArrayList<Integer>();//记录不为0的行号
                String[][] data_allrows = new String[paras.size() - 1][cols];//记录paras个长为cols的要素数据，且假设有效行都只有一行
                for (int pn = 0; pn < paras.size() - 1; pn++) {
//                    String[] splited = DataValues[pn + 1].split(" ");
                    String[] splited = DataValues.get(pn + 1).split(" ");
                    //Collections.addAll(ary, splited);
                    for (int i = 0; i < rows; i++) {
                        if (!splited[i * cols].equals("99999.0"))//注意一下这个缺省值
                        {
                            useRow.add(i);
                            for (int jj = 0; jj < cols; jj++) {
                                data_allrows[pn][jj] = splited[i * cols + jj];
                            }
                        }
                    }
                }

                double[][] maxValues = new double[paras.size() - 1][2];
                double[][] minValues = new double[paras.size() - 1][2];
                for (int i = 0; i < paras.size() - 1; i++) {
                    for (int j = 0; j < 2; j++) {
                        maxValues[i][j] = Double.MIN_VALUE;
                        minValues[i][j] = Double.MAX_VALUE;
                    }
                }
                double tmpValue = 0;
                for (int i = 0; i < maxValues.length; i++) {
                    for (int j = 0; j < cols; j++) {
                        tmpValue = Double.valueOf(pres_allR[useRow.get(i)][j]); // 计算在参数有记录的列的最大最小值
                        if (tmpValue != 99999)//注意一下这个缺省值
                        {
                            if (tmpValue > maxValues[i][0]) {
                                maxValues[i][0] = tmpValue;
                            }
                            if (tmpValue < minValues[i][0]) {
                                minValues[i][0] = tmpValue;
                            }
                        }
                    }
                }

                for (int i = 0; i < maxValues.length; i++) {
                    for (int j = 0; j < cols; j++) {
                        tmpValue = Double.valueOf(data_allrows[i][j]);
                        if (tmpValue != 99999)//注意一下这个缺省值
                        {
                            if (tmpValue > maxValues[i][1]) {
                                maxValues[i][1] = tmpValue;
                            }
                            if (tmpValue < minValues[i][1]) {
                                minValues[i][1] = tmpValue;
                            }
                        }
                    }
                }

                /////////////////////////////////
                //生成json开始
                /////////////////////////////////
                JSONObject json = new JSONObject();
                JSONObject prod = new JSONObject();
                JSONObject[] members = new JSONObject[paras.size() - 1];
                JSONObject[] maxMembers = new JSONObject[paras.size() - 1];
                JSONObject[] minMembers = new JSONObject[paras.size() - 1];
                //memberIndenties在这里即paras
                try {
                    double tmpP = 0;
                    double tmpV = 0;
                    for (int i = 0; i < paras.size() - 1; i++) {
                        if (!paras.get(i + 1).contains("R")) {
                            maxMembers[i] = new JSONObject();
                            minMembers[i] = new JSONObject();
                            maxMembers[i].put("P", String.valueOf(maxValues[i][0]));
                            minMembers[i].put("P", String.valueOf(minValues[i][0]));
                            maxMembers[i].put(paras.get(i + 1), String.valueOf(maxValues[i][1]));
                            minMembers[i].put(paras.get(i + 1), String.valueOf(minValues[i][1]));
                        } else if (paras.get(i + 1).contains("R1")) {
                            maxMembers[i] = new JSONObject();
                            minMembers[i] = new JSONObject();
                            maxMembers[i].put("P", String.valueOf(maxValues[i][0]));
                            minMembers[i].put("P", String.valueOf(minValues[i][0]));
                            maxMembers[i].put(paras.get(i + 1), String.valueOf(maxValues[i][1]));
                            minMembers[i].put(paras.get(i + 1), String.valueOf(minValues[i][1]));
                            for (int rn = 1; rn < 4; rn++) {
                                maxMembers[i].put(paras.get(i + rn + 1), String.valueOf(maxValues[i + rn][1]));
                                minMembers[i].put(paras.get(i + rn + 1), String.valueOf(minValues[i + rn][1]));
                            }
                        }
                    }
                    //各类members[].put(type、max、min、paras和data)
                    ArrayList<Double> pData = new ArrayList<Double>();
                    ArrayList<Double> vData = new ArrayList<Double>();
                    for (int i = 0; i < paras.size() - 1; i++) {
                        if (!paras.get(i + 1).contains("R")) {
                            members[i] = new JSONObject();
                            members[i].put("type", combineType.get(i));
                            members[i].put("max", maxMembers[i]);
                            members[i].put("min", minMembers[i]);
                            tmpP = 0;
                            tmpV = 0;
                            pData = new ArrayList<Double>();
                            vData = new ArrayList<Double>();
                            for (int j = 0; j < cols; j++) {
                                tmpP = Double.valueOf(pres_allR[useRow.get(i)][j]);
                                tmpV = Double.valueOf(data_allrows[i][j]);
                                if (tmpP != 99999 && tmpV != 99999) {
                                    pData.add(tmpP);
                                    vData.add(tmpV);
                                }
                            }
                            members[i].put("P", pData);
                            members[i].put(paras.get(i + 1), vData);
                            //prod.put(各类members[])
                            prod.put(String.valueOf(i + 1), members[i]);
                        } else if (paras.get(i + 1).contains("R1")) {
                            members[i] = new JSONObject();
                            members[i].put("type", "8");

                            members[i].put("max", maxMembers[i]);
                            members[i].put("min", minMembers[i]);

                            for (int rn = 0; rn < 4; rn++) {
                                tmpP = 0;
                                tmpV = 0;
                                pData = new ArrayList<Double>();
                                vData = new ArrayList<Double>();
                                for (int j = 0; j < cols; j++) {
                                    tmpP = Double.valueOf(pres_allR[useRow.get(i + rn)][j]);
                                    tmpV = Double.valueOf(data_allrows[i + rn][j]);
                                    if (tmpP != 99999 && tmpV != 99999) {
                                        pData.add(tmpP);
                                        vData.add(tmpV);
                                    }
                                }

                                if (rn == 0) {
                                    members[i].put("P", pData);//只加一次
                                }
                                members[i].put(paras.get(i + rn + 1), vData);
                                if (rn == 3) {
                                    prod.put(String.valueOf(i + 1), members[i]);
                                }
                            }
                        }
                    }
                    json.put("profiledata", prod);

                    String ArgoJsonPath = XmlUtils.ReadXml("ArgoJsonPath");
//                    String NCLocalPath = proNC_path.substring(0, proNC_path.substring(0, proNC_path.lastIndexOf('/')).lastIndexOf('/')) + "/json";
                    createJsonFile("", json.toString(), profile_name + ".json", ArgoJsonPath + platform_number);

                    /////////////////////////////////
                    // 更新数据库
                    String insertPathSQL = "Update bgc_profile set "
                            + "para_code='" + DataVars.toString() + "', " // 不确定用DataVars还是paras
                            + "nc_path='" + proNC_path + "', "
                            + "json_path='" + ArgoJsonPath + platform_number + "/" + profile_name + ".json'"
                            + " where profile_name='" + profile_name + "'";
                    DBUtils.getInstance().executeUpdate(insertPathSQL);
                    /////////////////////////////////

                } catch (JSONException ioej) {
                    LogUtils.getInstance().logException("Json generating" + proNC_path + ioej);
                }
                /////////////////////////////////
                //生成json结束
                /////////////////////////////////
            } catch (IOException ioe) {
                LogUtils.getInstance().logException("trying to open " + proNC_path + ioe);
            } catch (IllegalArgumentException ioe2) {
                LogUtils.getInstance().logException("profile miss para " + proNC_path + ioe2);
            } catch (IndexOutOfBoundsException ibe) {
                LogUtils.getInstance().logException("profile index out of ounds " + proNC_path + ibe);
            } catch (NullPointerException npe) { // 认为是由于浮标元数据导入出了问题
                LogUtils.getInstance().logException("NullPointerException " + proNC_path + npe);
            } finally {
                if (null != ncfile) try {
                    DataVars.clear();
                    ncfile.close();
                } catch (IOException ioe) {
                    LogUtils.getInstance().logException("trying to close " + proNC_path + ioe);
                }
            }
        }
    }

    public void addLoLat(ArrayList<String> profileList) {
        String SQL = "";
        for (String proNC_path : profileList) { // F:/EnglishPath/0Paper/BgcData/Download/csio/2902753/profiles/MR2902753_001.nc
//        String proNC_path = "F:/EnglishPath/0Paper/BgcData/Download/csio/2902754/profiles/MR2902754_001.nc";
//            String profile_name = proNC_path.substring(proNC_path.indexOf("MR") + 2, proNC_path.indexOf(".nc"));
            String profile_name = "D:/BGCAgro/Download/" + proNC_path;
            String profile_name_0 = proNC_path.substring(proNC_path.lastIndexOf('/') + 3, proNC_path.indexOf(".nc"));
            String[] ProfileValues = readNC(profile_name, addlonlatVars);
            try {
                if (ProfileValues.length == addlonlatVars.length) {
                    if (ProfileValues[0].equals("99999.0") || ProfileValues[1].equals("99999.0")) {
                        continue;
                    }
                    SQL = "UPDATE bgc_profile set time_write=to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS'),";
                    for (int i = 0; i < addlonlatVars.length; i++) {
                        LogUtils.getInstance().logInfo(addlonlatVars[i] + " : " + ProfileValues[i]);
                        if (addlonlatVars[i].contains("time_") || addlonlatVars[i].contains("date") || addlonlatVars[i].contains("DATE")) {
                            SQL += addlonlatVars[i] + "=to_timestamp('" + ProfileValues[i] + "','YYYYMMDDHH24MISS'),";
                        } else {
                            SQL += addlonlatVars[i] + "='" + ProfileValues[i] + "',";
                        }
                    }
                    SQL = SQL.substring(0, SQL.length() - 1) + " where profile_name='" + profile_name_0 + "' ";
                    DBUtils.getInstance().executeUpdate(SQL);
                }
//            String platform_number = proNC_path.substring(proNC_path.indexOf("MR") + 2, proNC_path.indexOf("_"));
            } catch (NullPointerException npe) { // 认为是由于浮标元数据导入出了问题
                LogUtils.getInstance().logException("NullPointerException" + proNC_path + npe);
            } catch (Exception npe) { // 认为是由于浮标元数据导入出了问题
                LogUtils.getInstance().logException("Exception:" + proNC_path + npe);
            } finally {
                if (null != ncfile) try {
                    DataVars.clear();
                    ncfile.close();
                } catch (IOException ioe) {
                    LogUtils.getInstance().logException("trying to close " + proNC_path + ioe);
                }
            }
        }
    }

    //读取nc文件中的指定字段
    public String[] readNC(String filepath, String[] VarNames) {
        String[] Values = new String[VarNames.length];
        LogUtils.getInstance().logInfo("Opening:" + filepath);
        try {
            ncfile = NetcdfFile.open(filepath);
            for (int i = 0; i < VarNames.length; i++) {
                Variable thisV = ncfile.findVariable(VarNames[i]);
                if (thisV == null) {
                    throw new IllegalArgumentException();
                }
                if (thisV.getDimensions().size() > 1) {
                    Array temp = thisV.read();
                    Values[i] = temp.toString().replace(" ", "").replace(",", ";");
                } else {
                    if (thisV.getDataType() == DataType.DOUBLE) {
                        Values[i] = String.valueOf(thisV.read().getObject(0).toString());
//                        readScalarDouble()
//                        Values[i] = "25315.2";
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

    /**
     * 创建Json文件并上传至HDFS，同时复制剖面文件
     *
     * @param profileName
     * @param jsonContent
     * @param jsonName
     * @param localPath
     */
    protected void createJsonFile(String profileName, String jsonContent, String jsonName, String localPath) {
        try {
            File folder = new File(localPath);
            if (!folder.exists()) {
                folder.mkdirs();
            }
            //创建json文件
            File file = new File(localPath + "/" + jsonName);
            if (file.exists()) {
                file.delete();
            }

            file.createNewFile();
            FileWriter writer = new FileWriter(file, true);
            writer.write(jsonContent);
            writer.close();

//            //复制剖面文件
//            file = new File(localPath + "\\" + profileName);
//            if (file.exists()) {
//                file.delete();
//            }
            LogUtils.getInstance().logInfo("Creating: " + localPath + "/" + jsonName + "\n");

        } catch (Exception e) {
            LogUtils.getInstance().logException(e, jsonName + "创建Json失败");
        }
    }
}
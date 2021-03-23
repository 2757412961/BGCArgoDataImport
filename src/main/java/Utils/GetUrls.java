package Utils;

import ucar.ma2.Array;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class GetUrls {

    private static GetUrls GetUrlsInstance = null;
    private static ArrayList<String> metaUrl = new ArrayList<String>();
    private static ArrayList<String> profileUrl = new ArrayList<String>();
    private static ArrayList<String> profileUrlSRes = new ArrayList<String>();
    private static ArrayList<String> profileUrlNotDownload = new ArrayList<String>();
    private static ArrayList<String> profileUrlDownload = new ArrayList<String>();

    protected GetUrls() {

    }

    public static GetUrls getInstance() {
        if (GetUrlsInstance == null) {
            GetUrlsInstance = new GetUrls();
        }

        return GetUrlsInstance;
    }


    public String getNow() {
        Date dat = new Date();
        Calendar calendar = Calendar.getInstance();
        dat = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String dateNowStr = sdf.format(dat);
        return dateNowStr;
    }

    public void initFile(String weekMetaXml, String weekProfXml) {
//        System.out.println(GetUrls.getInstance().getNow());
        LogUtils.getInstance().logInfo("[weekly routine] Current Time: " + GetUrls.getInstance().getNow());

        String downloadFileDirectory = XmlUtils.ReadXml("ArgoDownLoadPath");
        String metaUrl = XmlUtils.ReadXml(weekMetaXml);
        String profUrl = XmlUtils.ReadXml(weekProfXml);
        String metaFileName = metaUrl.substring(metaUrl.lastIndexOf('/') + 1);
        String profFileName = profUrl.substring(profUrl.lastIndexOf('/') + 1);
        String metaPath = downloadFileDirectory + metaFileName;
        String profPath = downloadFileDirectory + profFileName;
        File metaFile = new File(metaPath);
        File profFile = new File(profPath);

        if (metaFile.exists())
            metaFile.delete();
        if (profFile.exists())
            profFile.delete();
    }

    // aoml/1900022/1900022_meta.nc
    public ArrayList<String> getMetaUrl(String ProfileIndex) {
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_merge-profile_index.txt";
        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_synthetic-profile_index.txt";
        File file = new File(ProfileIndex);
        String fileName = file.getName();

        ArrayList<String> bgcProfileUrl = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                if (tmpLine.contains("D.nc")) {
                    tmpLine = br.readLine();
                    continue; //跳过反向检测的浮标
                }
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 10) {
                    bgcProfileUrl.add(splited[0]);
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }

        for (int i = 0; i < bgcProfileUrl.size(); i++) {
            String[] splited = bgcProfileUrl.get(i).split("/");
            if (splited.length > 1) {
                metaUrl.add(splited[0] + "/" + splited[1] + "/" + splited[1] + "_meta.nc");
            }
        }

        Set<String> middleHashSet = new LinkedHashSet<String>(metaUrl);
        metaUrl = new ArrayList<String>(middleHashSet);

        return metaUrl;
    }


    public ArrayList<String> getMetaUrl_Res(String ProfileIndex) {
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_merge-profile_index.txt";
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_synthetic-profile_index.txt";
        File file = new File(ProfileIndex);
        String fileName = file.getName();

        String downloadFilePath = XmlUtils.ReadXml("ArgoDownLoadPath");
        ArrayList<String> bgcProfileUrl = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                if (tmpLine.contains("D.nc")) {
                    tmpLine = br.readLine();
                    continue; //跳过反向检测的浮标
                }
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 10) {
                    bgcProfileUrl.add(splited[0]);
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }

        for (int i = 0; i < bgcProfileUrl.size(); i++) {
            String[] splited = bgcProfileUrl.get(i).split("/");
            if (splited.length > 1) {
                String url = splited[0] + "/" + splited[1] + "/" + splited[1] + "_meta.nc";
                if (!new File(downloadFilePath + url).exists()) {
                    metaUrl.add(url);
                }
            }
        }

        Set<String> middleHashSet = new LinkedHashSet<String>(metaUrl);
        metaUrl = new ArrayList<String>(middleHashSet);

        return metaUrl;
    }

    // csio/2902753/profiles/MR2902753_001.nc
    public ArrayList<String> getProfileUrl(String ProfileIndex) {
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_merge-profile_index.txt";
        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_synthetic-profile_index.txt";
        File file = new File(ProfileIndex);
        String fileName = file.getName();

        ArrayList<String> bgcProfile = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                if (tmpLine.contains("D.nc")) {
                    tmpLine = br.readLine();
                    continue; //跳过反向检测的浮标
                }
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 10) {
                    profileUrl.add(splited[0]);
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }

        return profileUrl;
    }

    // 获得S文件的，除去原先的M文件后剩余的url列表
    public ArrayList<String> getProfileUrl_S_Res(String ProfileIndex) {
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_synthetic-profile_index.txt";
        File file = new File(ProfileIndex);

        String jsonFilePath = XmlUtils.ReadXml("ArgoJsonPath");
        String downloadFilePath = XmlUtils.ReadXml("ArgoDownLoadPath");
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                if (tmpLine.contains("D.nc")) {
                    tmpLine = br.readLine();
                    continue; //跳过反向检测的浮标
                }
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 10) {
                    String profile_name = splited[0].substring(splited[0].indexOf(".nc") - 11, splited[0].indexOf(".nc"));
                    String platform_number = splited[0].substring(splited[0].indexOf('/') + 1, splited[0].indexOf("/profiles"));

                    if (
//                            (new File(downloadFilePath + splited[0]).exists()
//                            || new File(downloadFilePath + splited[0].replace('S', 'M')).exists()
//                            || new File(downloadFilePath + splited[0].replace('S', 'M').replace('D', 'R')).exists()
//                            || new File(downloadFilePath + splited[0].replace('S', 'M').replace('R', 'D')).exists()
//                    ) &&
                            new File(jsonFilePath + platform_number + '/' + profile_name + ".json").exists()) {
                        // do nothing
                    } else {
                        profileUrlSRes.add(splited[0]);
                    }
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, file.getName() + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }

        return profileUrlSRes;
    }

    public ArrayList<String> getProfileUrlNotDownload(String ProfileIndex, String fileNameProfileUrlNotDownload) {
        // csio/2902753/profiles/MR2902753_001.nc
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_merge-profile_index.txt";
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_synthetic-profile_index.txt";
//        String fileNameProfileUrlNotDownload = "D:\\BGCAgro\\Download\\profile_not_download.txt";
        String downloadFilePath = XmlUtils.ReadXml("ArgoDownLoadPath");

        File file = new File(ProfileIndex);
        String fileName = file.getName();
        File fileProfileUrlNotDownload = new File(fileNameProfileUrlNotDownload);
        try {
            FileWriter temp = new FileWriter(fileProfileUrlNotDownload);
            temp.write("");
            temp.flush();
            temp.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList<String> bgcProfile = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                if (tmpLine.contains("D.nc")) {
                    tmpLine = br.readLine();
                    continue; //跳过反向检测的浮标
                }
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 10) {
                    FileWriter fw = new FileWriter(fileProfileUrlNotDownload, true);
                    BufferedWriter bw = new BufferedWriter(fw);

                    File localFile = new File(downloadFilePath + splited[0]);
                    if (localFile.exists()) {
//                        bw.write("# " + tmpLine);
                    } else {
                        bw.write(tmpLine);
                        profileUrlNotDownload.add(splited[0]);
                    }

                    bw.write("\r\n");//换行
                    bw.flush();
                    bw.close();
                    fw.close();
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }

        return profileUrlNotDownload;
    }

    public ArrayList<String> getProfileUrlDownload(String ProfileIndex) {
        // csio/2902753/profiles/MR2902753_001.nc
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_merge-profile_index.txt";
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_synthetic-profile_index.txt";
        String downloadFilePath = XmlUtils.ReadXml("ArgoDownLoadPath");
        String jsonFilePath = XmlUtils.ReadXml("ArgoJsonPath");

        File file = new File(ProfileIndex);
        String fileName = file.getName();

        ArrayList<String> bgcProfile = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                if (tmpLine.contains("D.nc")) {
                    tmpLine = br.readLine();
                    continue; //跳过反向检测的浮标
                }
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 10) {
//                if (splited.length == 10 && splited[0].contains("coriolis")) {
                    File localFile = new File(downloadFilePath + splited[0]);
//                   String platform_number =  splited[0].substring(splited[0].indexOf("/") + 1, splited[0].indexOf("profiles") - 1);
                    String platform_number = splited[0].substring(splited[0].lastIndexOf('/') + 3, splited[0].indexOf("_"));
                    String profile_name = splited[0].substring(splited[0].lastIndexOf('/') + 3, splited[0].indexOf(".nc"));
                    File jsonFile = new File(jsonFilePath + platform_number + '/' + profile_name + ".json");
                    if (localFile.exists() && !jsonFile.exists()) {
                        profileUrlDownload.add(splited[0]);
                    }
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }

        return profileUrlDownload;
    }

    public ArrayList<String> getLonLatList(String txtfile) {
        File txtname = new File(txtfile);
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(txtname);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null) {
                profileUrlNotDownload.add(tmpLine);
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, txtfile + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }

        return profileUrlNotDownload;
    }
}

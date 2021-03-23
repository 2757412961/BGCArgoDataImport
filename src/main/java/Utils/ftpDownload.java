package Utils;

/**
 * @author ：Jesse.Qi
 * @date ：Created in 2019/9/11 21:44
 * @description：
 * @modified By：
 * @version: $
 */

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.*;

public class ftpDownload {
    /**
     * ftp连接
     */
    private static FTPClient ftpClient = new FTPClient();

    /**
     * ftp服务器地址
     */
    private static String hostName = XmlUtils.ReadXml("ArgoIP");
    /**
     * ftp端口
     */
    private static int port = 21;
    /**
     * 登录名
     */
    private static String userName = XmlUtils.ReadXml("ArgoLogName");
    ;// 匿名登录
    /**
     * 登录密码
     */
    private static String password = XmlUtils.ReadXml("ArgoPassWord");
    ;// 匿名则随意
    /**
     * 需要访问的远程目录
     */
    private static String remoteDir = XmlUtils.ReadXml("ArgoRemoteDir");
    /**
     * 需要访问的远程文本目录
     */
    private static String argoRemoteDirTxt = XmlUtils.ReadXml("ArgoRemoteDirTxt");
    /**
     * 需要访问的远程数据目录
     */
    private static String argoRemoteDirDac = XmlUtils.ReadXml("ArgoRemoteDirDac");
    /**
     * 保存路徑
     */
    private static String downloadFilePath = XmlUtils.ReadXml("ArgoDownLoadPath");
    /**
     * 记录表名
     */
    private static String RecordTable = XmlUtils.ReadXml("RecordTable");
    /**
     * 最多重连次数
     */
    static int maxTry = 5;

    /**
     * 保持 FTP 连接
     */
    public static void touch()  {
        try {
            connect();
            ftpClient.sendNoOp();
//            LogUtils.getInstance().logInfo("Touch Successfully!");
        } catch (IOException e) {
            LogUtils.getInstance().logInfo("Can not send NoOp, keep alive failed!");
            e.printStackTrace();
        }
    }

    /**
     * 下载txt文件，返回文件路径
     */
    public static String Download_txt(String txt_name) {
        String Url = XmlUtils.ReadXml(txt_name);
//    <allmetaIndex>ftp://ftp.ifremer.fr/ifremer/argo/ar_index_global_meta.txt</allmetaIndex>
//    <MRIndex>ftp://ftp.ifremer.fr/ifremer/argo/argo_merge-profile_index.txt</MRIndex>
//    <SRIndex>ftp://ftp.ifremer.fr/ifremer/argo/argo_synthetic-profile_index.txt</SRIndex>
//        "allmetaIndex"
//        "bgcIndex"
//        "allmetaIndex_week"
//        "allprofIndex_week"
        String downloaded = "";

        try {
            if (!ftpClient.isConnected()) {
                connect();
            }
//            ftpClient.changeWorkingDirectory(new String("ifremer/argo/dac".getBytes("UTF-8"),"ISO-8859-1"));
            ftpClient.enterLocalPassiveMode();//设置FTP连接模式

            String fileName = Url.substring(Url.lastIndexOf('/') + 1);
            FTPFile files[] = ftpClient.listFiles(new String((argoRemoteDirTxt + fileName).getBytes("UTF-8"), "ISO-8859-1"));
            if (files.length >= 1) {
                files[0].setLink(argoRemoteDirTxt);

                ExcuteDownLoad(files[0]);
                downloaded = downloadFilePath + files[0].getName();
            }

            closeConnections();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return downloaded;
    }

    /**
     * 下载nc文件，返回文件路径
     */
    public static ArrayList<String> Download_list(ArrayList<String> urls) {
        // makemetaUrl   aoml/1900022/1900022_meta.nc
        // getProfileUrl aoml/1900722/profiles/MD1900722_001.nc
        ArrayList<String> downloaded = new ArrayList<String>();
//        int reconnect=0;

        final ExecutorService exec = Executors.newFixedThreadPool(1);

        for (String url : urls) {
            String fileName = url;
            File localFile = new File(downloadFilePath + fileName);

            if (localFile.exists()) { // 如果本地文件存在，则跳过ftp下载
//            if (false) { // 因为还有不完整的文件，所以重新检测一遍，ExecuteDownload会执行断点续传
                downloaded.add(downloadFilePath + fileName);
                LogUtils.getInstance().logInfo(String.format("[ftp] Local file:%s is already exits", fileName));
            } else {
                //开始执行耗时操作
                Callable<String> call = new Callable<String>() {
                    public String call() throws Exception {
                        if (!ftpClient.isConnected()) {
                            connect();
                        }

                        FTPFile files[] = ftpClient.listFiles(new String((argoRemoteDirDac + fileName).getBytes("UTF-8"), "ISO-8859-1"));
                        if (files.length >= 1) {
                            files[0].setLink(argoRemoteDirDac);
                            files[0].setName(fileName);

                            ExcuteDownLoad(files[0]);
                            downloaded.add(downloadFilePath + files[0].getName());
                            //////////////////////////////////////////////////////////////////
                            // 更新数据库 添加nc的文件路径，因为如果后面生产json报错IllegalArgumentException，就不会更新nc路径。PS: 这部分代码可以删掉，不影响结果
                            if (fileName.split("/").length == 3) {
                                String platform_number = fileName.substring(fileName.lastIndexOf('/') + 1, fileName.indexOf("_"));
                                String UpdatePathSQL = "Update bgc_metadata set nc_path='" + downloadFilePath + fileName
                                        + "' where platform_number='" + platform_number + "'";
                                DBUtils.getInstance().executeUpdate(UpdatePathSQL);
                            } else if (fileName.split("/").length == 4) {
//                                String profile_name = fileName.substring(fileName.lastIndexOf('/') + 3, fileName.indexOf(".nc"));
                                String profile_name = fileName.substring(fileName.indexOf(".nc") - 11, fileName.indexOf(".nc"));
                                String UpdatePathSQL = "Update bgc_profile set nc_path='" + downloadFilePath + fileName
                                        + "' where profile_name='" + profile_name + "'";
                                DBUtils.getInstance().executeUpdate(UpdatePathSQL);
                            }
                            //////////////////////////////////////////////////////////////////
                        }

                        return "线程执行完成.";
                    }
                };

                try {
                    Future<String> future = exec.submit(call);
                    String obj = future.get(10 * 60 * 1000, TimeUnit.MILLISECONDS); //任务处理超时时间设为 120 秒
                } catch (TimeoutException ex) {
                    ex.printStackTrace();
                    LogUtils.getInstance().logException(ex, "[ftp] " + downloadFilePath + fileName + " 下载超时");
                    if (localFile.exists()) {
                        localFile.delete(); // 把未下载完的文件删除，确保所有的都是完整文件，就不需要向数据库中添加字段了
                    }
                    closeConnections();
                    ftpClient = new FTPClient();
                    while (!connect()) {
                        LogUtils.getInstance().logInfo("[ftp] 正在尝试重连 请等待30秒！");
                        try {
                            Thread.sleep(30 * 1000);//过30 秒再重连
                        } catch (InterruptedException ie) {
                        }
                    }
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e, "[ftp] " + downloadFilePath + fileName + " 下载失败");
                    if (localFile.exists()) {
                        localFile.delete(); // 把未下载完的文件删除，确保所有的都是完整文件，就不需要向数据库中添加字段了
                    }
                    closeConnections();
                    ftpClient = new FTPClient();
                    while (!connect()) {
                        LogUtils.getInstance().logInfo("[ftp] 正在尝试重连 请等待30秒！");
                        try {
                            Thread.sleep(30 * 1000);//过30 秒再重连
                        } catch (InterruptedException ie) {
                        }
                    }
                }
            } // else Finished
        }

        exec.shutdown(); // 关闭线程池

        return downloaded;
    }

    /**
     * 连接FTP服务器，这一步骤单独拥有5次最大重连次数
     *
     * @throws SocketException
     * @throws IOException
     */
    public static boolean connect() {
        if (ftpClient.isConnected()) {
            closeConnections();
        }
        int curTry = 1;
        while (curTry <= maxTry) {
            try {
                ftpClient = new FTPClient();
                ftpClient.setConnectTimeout(15000);
                // 设置Socket 连接超时, 2019-12-21 测试
                ftpClient.setDataTimeout(15000);
//                ftpClient.setDefaultTimeout(15000);
//                ftpClient.setSoTimeout(20000);
                // 设置Socket 连接超时, 2019-12-21 测试 End
                ftpClient.connect(hostName, port);
                if (ftpClient.login(userName, password)) {
                    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                    // ftpClient.setRemoteVerificationEnabled(false);
                    ftpClient.enterLocalPassiveMode();
                    // ftpClient.setControlEncoding("GBK");
                    if (FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
                        LogUtils.getInstance().logInfo("[ftp] Connect " + hostName + " Successed");
                        return true;

                    }
                }
            } catch (SocketException e) {
                e.printStackTrace();
                LogUtils.getInstance().logException(e, "[ftp]");
            } catch (IOException e) {
                e.printStackTrace();
                LogUtils.getInstance().logException(e, "[ftp]");
            }
            closeConnections();
            // LogUtils.getInstance().logInfo("[ftp] Connect " + hostName + " Failed");
            LogUtils.getInstance().logInfo("[ftp] Connect      " + hostName + " Failed");
            LogUtils.getInstance().logException("[ftp] Connect      " + hostName + " Failed");
            curTry++;
            if (curTry <= maxTry) {
                // LogUtils.getInstance().logInfo("[ftp] Try reconnect " + hostName + " —— "
                // + curTry + "times");
                LogUtils.getInstance().logInfo("[ftp] Try reconnect " + hostName + " —— " + curTry + "times");
            }
        }
        return false;
    }

    /**
     * 断开与远程服务器的连接
     *
     * @throws IOException
     */
    public static void closeConnections() {
        try {
            if (ftpClient.isConnected()) {
                ftpClient.disconnect();
            }
        } catch (IOException e) {
            // LogUtils.getInstance().logInfo("[ftp] DisConnect Failed:" + e.getMessage());
            LogUtils.getInstance().logException("[ftp] DisConnect Failed:" + e.getMessage());
        }
    }

    /**
     * 从FTP服务器上下载文件
     *
     * @param file 特定文件名
     * @throws IOException
     */
    public static boolean ExcuteDownLoad(FTPFile file) throws IOException {
        boolean res = false;
        String filename = "";
        String ftpFilePath = "";
        try {
            if (!ftpClient.isConnected()) {
                connect();
            }
            filename = file.getName();// 对象文件名
            ftpFilePath = file.getLink();// 对象在ftp中的路径
//            DBInteraction(filename, "downloading");
            String FullURL = ftpFilePath + filename;// 完整下载路径URL // remoteDir 改为了 ftpFilePath
            long lRemoteSize = file.getSize();// 对象文件大小

            // 如果文件夹不存在则自动创建文件夹
            File localFile = new File(downloadFilePath + filename.replace('/', '\\'));// 创建本地文件类
            File fileParent = localFile.getParentFile();
            if (!fileParent.exists()) {
                fileParent.mkdirs();
            }

            int curTry = 1;// 尝试次数

            if (!localFile.exists()) {
                if (DataDownLoad(filename, localFile, FullURL, 0L, lRemoteSize)) {
                    return true;
                }
            }
            long localSize = localFile.length();
            // 判断本地文件大小是否大于远程文件大小
            if (localSize >= lRemoteSize) {
                // LogUtils.getInstance().logInfo("[ftp] Local file:" + filename + " is
                // already exist");
                LogUtils.getInstance().logInfo("[ftp] Local file:" + filename + " is already exist");
                return true;
            } // 断点续传或零点重下
            while (curTry <= maxTry) {
                if (DataDownLoad(filename, localFile, FullURL, localSize, lRemoteSize)) {
                    return true;
                } else {
                    connect();
                }
                curTry++;
            }
        } catch (Exception e) {
            LogUtils.getInstance().logInfo("[ftp] Download " + filename + " Failed");
            // e.printStackTrace();
            LogUtils.getInstance().logException(e, "[ftp]");
            // LogUtils.getInstance().logInfo("[ftp] Download " + filename + " Failed");
            return false;
        }
        LogUtils.getInstance().logInfo("[ftp] Download " + filename + " Failed");
        LogUtils.getInstance().logException("[ftp] Download " + filename + " Failed");
        return res;
    }

    /**
     * 具体下载步骤,支持断点续传，上传百分比汇报
     *
     * @throws IOException
     */
    public static boolean DataDownLoad(String filename, File localFile, String FullURL, long localSize,
                                       long lRemoteSize) throws IOException {
        boolean res = false;
        try {
            if (!ftpClient.isConnected()) {
                connect();
            }
            if (localSize == 0) { // 正常下载
                // System.out.println("开始文件" + filename + "的下载");
//                LogUtils.getInstance().logInfo("开始文件" + filename + "的下载"); // 中文乱码
                LogUtils.getInstance().logInfo("Start Download File:" + filename + " ");
                OutputStream out = new FileOutputStream(localFile);
                InputStream in = ftpClient.retrieveFileStream(new String(FullURL.getBytes("GBK"), "ISO-8859-1"));
                if (in == null) {
                    // System.out.println("[ftp] Can't get" + filename + " Data
                    // from target URL");
                    LogUtils.getInstance().logInfo("[ftp] Can't get" + filename + " Data from target URL");
                    out.close();
                    return false;
                } else {
                    byte[] bytes = new byte[1024];
                    long step = lRemoteSize / 100;
                    long process = 0;
                    long localSize0 = 0L;
                    int c;
                    while ((c = in.read(bytes)) != -1) {
                        out.write(bytes, 0, c);
                        localSize0 += c;
                        long nowProcess = localSize0 / step;
                        if (nowProcess > process) {
                            process = nowProcess;
                            if (process % 50 == 0) {
//                                System.out.println("[ftp] Download process :" + process + "%");
                                LogUtils.getInstance().logInfo("[ftp] Download process :" + process + "%");
                            }
                        }
                    }
                    if (localFile.length() == lRemoteSize) {
//                        System.out.println("[ftp] Download " + filename + " Successed");//2019-12-12 注释
                        LogUtils.getInstance().logInfo("[ftp] Download " + filename + " Successed");
                        res = true;
                    } else {
                        LogUtils.getInstance().logInfo("[ftp] Download " + filename + " Error");
                        LogUtils.getInstance().logException("[ftp] Download " + filename + " Error");
                        res = false;
                    }
                    in.close();
                    out.close();
                    ftpClient.completePendingCommand();
                }
            } else {
                // 断点续传
                // System.out.println("检测到未下载完的文件" + filename + "，正在开始断点续传");
                LogUtils.getInstance().logInfo("检测到未下载完的文件" + filename + "，正在开始断点续传");
                FileOutputStream out = new FileOutputStream(localFile, true);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                // int a = ftpClient.sendCommand("REST 1");
                ftpClient.setRestartOffset(localSize);
                InputStream in = ftpClient.retrieveFileStream(new String(FullURL.getBytes("GBK"), "ISO-8859-1"));
                if (in == null) {
                    // System.out.println("[ftp] Can't get" + filename + " Data
                    // from target URL");
                    LogUtils.getInstance().logInfo("[ftp] Can't get" + filename + " Data from target URL");
                    out.close();
                    return false;
                } else {
                    byte[] bytes = new byte[1024];
                    long step = lRemoteSize / 100;
                    long process = localSize / step;
                    int c;
                    while ((c = in.read(bytes)) != -1) {
                        out.write(bytes, 0, c);
                        localSize += c;
                        long nowProcess = localSize / step;
                        if (nowProcess > process) {
                            process = nowProcess;
                            if (process % 20 == 0)
                                // System.out.println("[ftp] Download process :"
                                // + process + "%");
//                                System.out.println("[ftp] Download process :" + process + "%");
                                LogUtils.getInstance().logInfo("[ftp] Download process :" + process + "%");
                        }
                    }
                    if (localFile.length() == lRemoteSize) {
                        LogUtils.getInstance().logInfo("[ftp] Download " + filename + " Successed");
                        res = true;
                    } else {
                        LogUtils.getInstance().logInfo("[ftp] Download " + filename + " Error");
                        LogUtils.getInstance().logException("[ftp] Download " + filename + " Error");
                        res = false;
                    }
                    in.close();
                    out.close();
                    ftpClient.completePendingCommand();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            // System.err.println("[ftp] " + e);
            LogUtils.getInstance().logException(e, "[ftp]");
        } finally {
            return res;
        }
    }

    /**
     * 与数据库进行下载状态的交互
     */
    private static void DBInteraction(String fileName, String status) {
        // 先查找
        String sql = "select * from " + RecordTable + " where filename='" + fileName + "'";
        int count = DBUtils.getInstance().executeNonQuery(sql);
        // 获取时间
        Date dat = new Date();
        Calendar calendar = Calendar.getInstance();
        dat = calendar.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String dateNowStr = sdf.format(dat);
        if (count > 0) {
            // 找到——更新
            sql = "update " + RecordTable + " set status='" + status + "',date_update=to_timestamp('" + dateNowStr
                    + "','YYYY-MM-DD hh24:mi:ss') where filename='" + fileName + "'";
            DBUtils.getInstance().executeUpdate(sql);
        } else {
            // 找不到——插入
            sql = "insert into " + RecordTable + " (filename,status,date_create) VALUES('" + fileName + "','" + status
                    + "',to_timestamp('" + dateNowStr + "','YYYY-MM-DD hh24:mi:ss'))";
            DBUtils.getInstance().execute(sql);
        }
    }

}

/**
 * @author ：Jesse.Qi
 * @date ：Created in 2019/9/10 10:29
 * @description：Main Class
 * @modified By：
 * @version: 1.1$
 */

import Utils.*;

import java.io.File;
import java.util.*;


public class main {
    public static String IsFirst = XmlUtils.ReadXml("IsFirst");

    public static void runTask() {
        // 通过了GetUrls类，实现在任意时刻就能够下载、入库、更新剩余文件，而不必每周自动运行。换句话说就是改成了手动运行
        // 下载 allmeta 和 SRIndex 的 Index文件
        String allIndex = "", SRIndex = "";
        while (allIndex == null || SRIndex == null ||
                !allIndex.contains("txt") || !SRIndex.contains("txt")) {
            allIndex = ftpDownload.Download_txt("allmetaIndex");
            SRIndex = ftpDownload.Download_txt("SRIndex");
            System.out.println(allIndex + "\n" + SRIndex);
        }

        if (IsFirst.equals("true")) {
            // 第一次入库

            // 读取文件，插入数据库。
            ReadTxt.getInstance().Read_allmetaIndex(allIndex);
            ReadTxt.getInstance().Read_bgcIndex(SRIndex);

            // 第一次插入数据库
            ArrayList<String> metafileuels = GetUrls.getInstance().getMetaUrl(SRIndex); // 获取剖面meta数据nc的url
            ReadMetaNC.getInstance().readFile(ftpDownload.Download_list(metafileuels));

            ArrayList<String> profileurls = GetUrls.getInstance().getProfileUrl(SRIndex); // 获取所有的剖面nc的url
            ReadProfileNC.getInstance().readFile(ftpDownload.Download_list(profileurls));
        } else {
            // 通过 GetUrls 类，可以在任意时刻获得还没有读取的nc文件。

            // 读取文件，插入数据库。如果原先有数据存在，则更新
            ReadTxt.getInstance().Read_allmetaIndex(allIndex);
            ReadTxt.getInstance().Read_bgcIndex(SRIndex);

            // 将剩余文件插入数据库
            ArrayList<String> metafileuels = GetUrls.getInstance().getMetaUrl_Res(SRIndex); // 获取剩余剖面meta数据nc的url
            ReadMetaNC.getInstance().readFile(ftpDownload.Download_list(metafileuels));

            ArrayList<String> profileurls = GetUrls.getInstance().getProfileUrl_S_Res(SRIndex); // 获取未生成json、未下载、没有M文件 的S剖面nc的url
            ReadProfileNC.getInstance().readFile(ftpDownload.Download_list(profileurls));
            // 多线程下载，需要把FTP改为可实例化才能进行下去，一个实例只能连接一个下载
//            int sliceNum = 7;
//            int sliceLen = profileurls.size() / sliceNum;
//            for (int i = 0; i <= sliceNum; i++) {
//                int sta = i * sliceLen;
//                int end = Math.min((i + 1) * sliceLen, profileurls.size());
//                MultiThread.getInstance().executeMultiThread(new Thread() {
//                    @Override
//                    public void run() {
////                        ReadProfileNC.getInstance().readFile(
//                                new FTPDownloadInstance().Download_list(
//                                        new ArrayList<>(profileurls.subList(sta, end))
//                                );
////                        );
//                    }
//                });
//            }
//            MultiThread.getInstance().shutdownMultiThread();

            LogUtils.getInstance().logInfo(
                    "本周入库结束：\n" +
                            "共入库" + metafileuels.size() + "条元数据\n" +
                            "共入库" + profileurls.size() + "条剖面数据\n"
            );
        }

        new File(SRIndex).delete();
    }

    public static void runTimeTask() {
        // 因为 FTPClient 是单例的，所以需要时刻保持其连接状态
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    ftpDownload.touch();
                    try {
                        Thread.sleep(3 * 60 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        LogUtils.getInstance().logInfo("Touch Sleep Failed");
                    }
                }
            }
        }).start();

        // 设置运行时间,可设置为每周一 9:00 开始执行，执行一周
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.DAY_OF_WEEK, 2); // 周一
        calendar.set(Calendar.HOUR_OF_DAY, 9); // 控制时
        calendar.set(Calendar.MINUTE, 0);      // 控制分
        calendar.set(Calendar.SECOND, 0);      // 控制秒

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                runTask();
            }
        }, calendar.getTime(), 7 * 24 * 60 * 60 * 1000); // 2s后开始执行，每{？}秒执行一次， 7d*24h*60m*60s
    }

    public static void main(String[] args) {
        // 定时下载 !!!!!!!!!!!
//        runTimeTask();

        // ------------------------- 测试部分 ---------------------------------
        LogUtils.getInstance().logInfo("Start Test");
//        ArrayList<String> test = new ArrayList<>();
//        test.add("F:/BGCArgo/BgcData/BGCAgro/Download/coriolis/6902954/profiles/SR6902954_003.nc"); // NullPointerException
//        test.add("F:/BGCArgo/BgcData/BGCAgro/Download/coriolis/6902954/profiles/MR6902954_003.nc"); // NullPointerException
//        test.add("F:/BGCArgo/BgcData/BGCAgro/Download/aoml/5904179/profiles/MD5904179_176.nc"); // NullPointerException
//        test.add("F:/BGCArgo/BgcData/BGCAgro/Download/aoml/5904179/profiles/SD5904179_176.nc"); // NullPointerException
//        test.add("D:/BGCAgro/Download/csiro/5905395/profiles/SD5905395_031.nc"); //
//        test = GetUrls.getInstance().getLonLatList("D:\\BGCAgro\\Download\\errorProfile.txt");
//        ReadProfileNC.getInstance().addLoLat(test);
//        ReadMetaNC.getInstance().readFile(test);
//        ReadProfileNC.getInstance().readFile(test);

        System.out.println("等待输入 nextDouble ---------------------------------------------------- ");
        new Scanner(System.in).nextDouble();
        // ----------------------- 测试部分结束 ---------------------------------

        // 原先的每周定时任务
        if (IsFirst.equals("true")) {
//            ReadTxt.getInstance().Read_allmetaIndex("D:\\BGCAgro\\Download\\0410index_global_meta.txt"); //2020.4更新
            ReadTxt.getInstance().Read_allmetaIndex(ftpDownload.Download_txt("allmetaIndex"));
            //下载ar_index_global_meta.txt,读取信息，填写all_metadata
//            ReadTxt.getInstance().Read_bgcIndex("D:\\BGCAgro\\Download\\0410merge-profile_index.txt"); //2020.4更新
            ReadTxt.getInstance().Read_bgcIndex(ftpDownload.Download_txt("MRIndex"));
            //下载argo_merge-profile_index.txt,读取信息，更新allmeta并复制其bgc的数据到bgcmeta、填写bgcprofile数据表

            //2020.4以下部分未更新！
            ReadMetaNC.getInstance().readFile(ftpDownload.Download_list(ReadTxt.getInstance().makemetaUrl()));
            //在程序中获取bgc_metadata的url列表，失败则须自动从数据库中获取！(ftpDownload)
            //下载bgc_metadata的nc文件 + 打开metaNC文件完善bgc_metadata表的详细信息
            ReadProfileNC.getInstance().readFile(ftpDownload.Download_list(ReadTxt.getInstance().getProfileUrl()));
            //在程序中获取bgc_profile的url列表，失败则须自动从数据库中获取！(ftpDownload)
            //下载bgc剖面文件 + 打开profileNC文件完善bgc_profile表的详细信息

        } else {
            // Do daliy things，计划设置为每24小时运行一次
            // 设置运行时间,可设置为每周一9:00开始执行，执行一周
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.DAY_OF_WEEK, 2); // 周一
            calendar.set(Calendar.HOUR_OF_DAY, 9); // 控制时
            calendar.set(Calendar.MINUTE, 0);      // 控制分
            calendar.set(Calendar.SECOND, 0);      // 控制秒

            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                public void run() {
                    GetUrls.getInstance().initFile("allmetaIndex_week", "allprofIndex_week");
                    // 删除源文件，如果本地文件比远程文件大，就不能覆盖

                    ReadTxt.getInstance().Read_allmetaIndex_week(ftpDownload.Download_txt("allmetaIndex_week"));
                    //下载本周ar_index_this_week_meta.txt,更新双meta表，标注新剖面为非bgc浮标
                    ReadTxt.getInstance().Read_bgcIndex_week(ftpDownload.Download_txt("allprofIndex_week"));
//                    ReadTxt.getInstance().Read_bgcIndex_week("D:\\BGCAgro\\Download\\ar_index_this_week_prof.txt"); // 测试用，可以删除
                    //下载本周ar_index_this_week_prof.txt,更新三表
                    //【剩了一个unsureList去判断！！！】

                    ReadMetaNC.getInstance().readFile(ftpDownload.Download_list(ReadTxt.getInstance().getmetaUrl()));
                    //在程序中获取bgc_metadata的url列表，失败则须自动从数据库中获取！(ftpDownload)
                    //下载bgc的新meta，更新bgc_meta表
                    ReadProfileNC.getInstance().readFile(ftpDownload.Download_list(ReadTxt.getInstance().getProfileUrl()));
                    //在程序中获取bgc_profile的url列表，失败则须自动从数据库中获取！(ftpDownload)
                    //下载bgc的新profile，更新bgc_profile表

                }
            }, calendar.getTime(), 7 * 24 * 60 * 60 * 1000); // 2s后开始执行，每{？}秒执行一次， 7d*24h*60m*60s
        }
    }
}
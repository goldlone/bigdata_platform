package cn.goldlone.bigdata_platform.utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Created by CN on 2018/11/22/0022 15:34 .
 */
public class FileUtil {

  /**
   * 保存为本地文件
   * @param is
   * @param tmpFilePath
   * @throws IOException
   */
  public static void saveTempFile(InputStream is, String tmpFilePath) throws IOException {
    byte[] bytes = new byte[1024];
    int len = 0;
    FileOutputStream fos = new FileOutputStream(tmpFilePath);
    while((len = is.read(bytes)) != -1) {
      fos.write(bytes, 0, len);
    }
    fos.close();
  }

}

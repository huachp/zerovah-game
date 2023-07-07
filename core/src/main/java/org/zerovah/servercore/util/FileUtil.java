package org.zerovah.servercore.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FileUtil {
	
	private static final Logger LOGGER = LogManager.getLogger(FileUtil.class);

	public static Properties readProperties(String path) {
		try (FileInputStream fis = new FileInputStream(path)) {
			Properties p = new Properties();
			p.load(fis);
			return p;
		} catch (Exception e) {
			LOGGER.error("", e);
			return null;
		}
	}

	public static String readFile(File file) {
		StringBuilder builder = new StringBuilder();
		try (FileInputStream fis = new FileInputStream(file);
			 InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
			 BufferedReader reader = new BufferedReader(isr)) {

			String tempString;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				// 显示行号
				builder.append(tempString).append("\n");
			}
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return builder.toString();
	}
	
	public static void saveFile(File file, String contents) {
		try (FileOutputStream fos = new FileOutputStream(file);
			 Writer writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {

			writer.write(contents);
			writer.flush();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}

	public static byte[] readFileToBytes(File file) {
		byte[] data = null;
		try (FileInputStream fis = new FileInputStream(file);
			 ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

			int b = -1;
			byte[] buf = new byte[2048];
			while ((b = fis.read(buf)) != -1) {
				bos.write(buf, 0, b);
			}
			data = bos.toByteArray();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return data;
	}
	
	public static List<File> getAllFiles(File folder) {
		List<File> files = new ArrayList<>();
		File[] inFiles = folder.listFiles();
		if (inFiles == null) {
			return null;
		}
		for (File file : inFiles) {
			if (file.isDirectory()) {
				List<File> fileList = getAllFiles(file);
				if (fileList == null) {
					continue;
				}
				files.addAll(fileList);
			} else {
				files.add(file);
			}
		}
		return files;
	}

	public static void saveFile(byte[] fileByte, String path) {
		try (FileOutputStream fos = new FileOutputStream(path)) {
			fos.write(fileByte);
			fos.flush();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
	}
	

}

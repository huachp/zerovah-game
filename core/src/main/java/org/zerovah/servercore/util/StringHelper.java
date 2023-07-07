package org.zerovah.servercore.util;

import java.util.List;

/**
 * string工具
 */
public class StringHelper {

    /**
     * 连接数组
     *
     * @param s       分隔符
     * @param objects
     */
    public static String join(String s, String... objects) {
        if (objects.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(objects[0].toString());
        for (int i = 1; i < objects.length; i++) {
            sb.append(s).append(objects[i]);
        }
        return sb.toString();
    }

    /**
     * 连接数组
     *
     * @param s  分隔符
     * @param ls
     */
    public static String join(String s, List ls) {
        if (ls.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(ls.get(0).toString());
        for (int i = 1; i < ls.size(); i++) {
            sb.append(s).append(ls.get(i));
        }
        return sb.toString();
    }

    /**
     * 大写首字母
     */
    public static String upFirst(String str) {
        return str.substring(0, 1).toUpperCase().concat(str.substring(1));
    }

    /**
     * 首字母大写
     *
     * @param str
     * @author Craig
     */
    public static String upFirst1(String str) {
        char[] strs = str.toCharArray();
        if ((strs[0] >= 'a' && strs[0] <= 'z')) {
            strs[0] -= 32;
            return String.valueOf(strs);
        } else {
            return upFirst(str);
        }
    }

    /**
     * 下划线风格转小写驼峰
     */
    public static String underlineToLowerCamal(String s) {
        String[] ss = s.split("_");
        for (int i = 1; i < ss.length; i++) {
            ss[i] = upFirst1(ss[i]);
        }
        return join("", ss);
    }

    /**
     * 下划线风格转大写驼峰
     */
    public static String underlineToUpperCamal(String s) {
        String[] ss = s.split("_");
        for (int i = 0; i < ss.length; i++) {
            ss[i] = upFirst1(ss[i]);
        }
        return join("", ss);
    }

    /**
     * 驼峰转下划线,未处理大小写
     */
    public static String camalToUnderline(String s) {
        StringBuilder sb = new StringBuilder();
        if (s.length() > 0) {
            sb.append(s.charAt(0));
        }
        for (int i = 1; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                sb.append("_");
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * 判断字符串为空（null或者""）
     *
     * <pre>
     * StringHelper.isEmpty(null) = true
     * StringHelper.isEmpty("") = true
     * StringHelper.isEmpty(" ") = false
     * StringHelper.isEmpty("bob") = false
     * StringHelper.isEmpty("  bob  ") = false
     * </pre>
     *
     * @param s
     * @return
     */
    public static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }

    /**
     * 判断字符串不为空（null或者""）
     *
     * <pre>
     * StringHelper.isNotEmpty(null) = false
     * StringHelper.isNotEmpty("") = false
     * StringHelper.isNotEmpty(" ") = true
     * StringHelper.isNotEmpty("bob") = true
     * StringHelper.isNotEmpty("  bob  ") = true
     * </pre>
     *
     * @param s
     * @return
     */
    public static boolean isNotEmpty(String s) {
        return !StringHelper.isEmpty(s);
    }

    /**
     * 判断字符串为空白字符串(null或者""或者" ")
     *
     * <pre>
     * StringHelper.isBlank(null) = true
     * StringHelper.isBlank("") = true
     * StringHelper.isBlank(" ") = true
     * StringHelper.isBlank("bob") = false
     * StringHelper.isBlank(" bob ") = false
     * </pre>
     *
     * @param s
     * @return
     */
    public static boolean isBlank(String s) {
        int strLen;
        if (s == null || (strLen = s.length()) == 0) {
            return true;
        }

        for (int i = 0; i < strLen; i++) {
            if (Character.isWhitespace(s.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断字符串不为空白字符串(null或者""或者" ")
     *
     * <pre>
     * StringHelper.isNotBlank(null) = false
     * StringHelper.isNotBlank("") = false
     * StringHelper.isNotBlank(" ") = false
     * StringHelper.isNotBlank("bob") = true
     * StringHelper.isNotBlank(" bob ") = true
     * </pre>
     *
     * @param s
     * @return
     */
    public static boolean isNotBlank(String s) {
        return !StringHelper.isBlank(s);
    }

    /**
     * 截取字符串
     *
     * <pre>
     * StringHelper.substringBefore("ajp_djp_gjp_j", "")  = ""
     * StringHelper.substringBefore("ajp_djp_gjp_j", null)  = ""
     * StringHelper.substringBefore("ajp_djp_gjp_j", "jp_")  = "a"
     * StringHelper.substringBefore("ajp_djp_gjp_j", "jk_")  = "ajp_djp_gjp_j"
     * </pre>
     *
     * @param str       被截取的字符串
     * @param separator 截取分隔符
     * @return
     */
    public static String substringBefore(String str, String separator) {
        if ((isEmpty(str)) || (separator == null)) {
            return str;
        }
        if (separator.isEmpty()) {
            return "";
        }
        int pos = str.indexOf(separator);
        if (pos == -1) {
            return str;
        }
        return str.substring(0, pos);
    }

    /**
     * 截取字符串
     *
     * <pre>
     * StringHelper.substringAfter("ajp_djp_gjp_j", "jp_")  = "defjp_ghi"
     * StringHelper.substringAfter("ajp_djp_gjp_j", "")  = "ajp_djp_gjp_j"
     * StringHelper.substringAfter("ajp_djp_gjp_j", null)  = "ajp_djp_gjp_j"
     * StringHelper.substringAfter("ajp_djp_gjp_j", "jk_")  = ""
     * </pre>
     *
     * @param str       被截取的字符串
     * @param separator 截取分隔符
     * @return
     */
    public static String substringAfter(String str, String separator) {
        if (isEmpty(str)) {
            return str;
        }
        if (separator == null) {
            return "";
        }
        int pos = str.indexOf(separator);
        if (pos == -1) {
            return "";
        }
        return str.substring(pos + separator.length());
    }

    /**
     * 截取字符串
     *
     * <pre>
     * StringHelper.substringBeforeLast("ajp_djp_gjp_j", "")  = "ajp_djp_gjp_j"
     * StringHelper.substringBeforeLast("ajp_djp_gjp_j", null)  = "ajp_djp_gjp_j"
     * StringHelper.substringBeforeLast("ajp_djp_gjp_j", "jk_")  = "ajp_djp_g"
     * StringHelper.substringBeforeLast("ajp_djp_gjp_j", "jp_")  = "ajp_djp_g"
     * </pre>
     *
     * @param str       被截取的字符串
     * @param separator 截取分隔符
     * @return
     */
    public static String substringBeforeLast(String str, String separator) {
        if ((isEmpty(str)) || (isEmpty(separator))) {
            return str;
        }
        int pos = str.lastIndexOf(separator);
        if (pos == -1) {
            return str;
        }
        return str.substring(0, pos);
    }

    /**
     * 截取字符串
     *
     * <pre>
     * StringHelper.substringAfterLast("ajp_djp_gjp_j", "")  = ""
     * StringHelper.substringAfterLast("ajp_djp_gjp_j", null)  = ""
     * StringHelper.substringAfterLast("ajp_djp_gjp_j", "jk_")  = ""
     * StringHelper.substringAfterLast("ajp_djp_gjp_j", "jp_")  = "j"
     * </pre>
     *
     * @param str       被截取的字符串
     * @param separator 截取分隔符
     * @return
     */
    public static String substringAfterLast(String str, String separator) {
        if (isEmpty(str)) {
            return str;
        }
        if (isEmpty(separator)) {
            return "";
        }
        int pos = str.lastIndexOf(separator);
        if ((pos == -1) || (pos == str.length() - separator.length())) {
            return "";
        }
        return str.substring(pos + separator.length());
    }

}

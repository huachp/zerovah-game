package org.zerovah.servercore.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;

/**
 * IP地址工具类
 *
 * @author huachp
 */
public class IPAddrUtil {

    public static byte[] textToNumericFormatV4(String ip) throws Exception {
        InetAddress address = InetAddress.getByName(ip);
        return address.getAddress();
    }

    public static boolean internalIp(String ip) throws Exception {
        byte[] addr = textToNumericFormatV4(ip);
        return internalIp(addr);
    }

    /**
     * tcp/ip协议中，专门保留了三个IP地址区域作为私有地址，其地址范围如下：
     *
     * 10.0.0.0/8：10.0.0.0～10.255.255.255
     * 172.16.0.0/12：172.16.0.0～172.31.255.255
     * 192.168.0.0/16：192.168.0.0～192.168.255.255
     *
     * @param addr IP地址数组表示形式
     * @return {@link Boolean}
     */
    public static boolean internalIp(byte[] addr) {
        final byte b0 = addr[0];
        final byte b1 = addr[1];
        //10.x.x.x/8
        final byte SECTION_1 = 0x0A;
        //172.16.x.x/12
        final byte SECTION_2 = (byte) 0xAC;
        final byte SECTION_3 = (byte) 0x10;
        final byte SECTION_4 = (byte) 0x1F;
        //192.168.x.x/16
        final byte SECTION_5 = (byte) 0xC0;
        final byte SECTION_6 = (byte) 0xA8;
        switch (b0) {
            case SECTION_1:
                return true;
            case SECTION_2:
                if (b1 >= SECTION_3 && b1 <= SECTION_4) {
                    return true;
                }
            case SECTION_5:
                switch (b1) {
                    case SECTION_6:
                        return true;
                }
            default:
                return false;

        }
    }

    public static String[] getInternalHostsIpv4() throws Exception {
        List<String> ipList = new ArrayList<>();
        Enumeration<NetworkInterface> netInterfaces = null;
        netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> ips = ni.getInetAddresses();
            while (ips.hasMoreElements()) {
                InetAddress address = ips.nextElement();
                if (address instanceof Inet4Address && internalIp(address.getAddress())) {
                    ipList.add(address.getHostAddress());
                }
            }
        }
        Collections.sort(ipList);
        return ipList.toArray(new String[0]);
    }


    public static void main(String[] args) throws Exception {
        String ip = "192.168.10.219";
        System.out.println("IP:" + ip + " is internal ip? -> " + internalIp(ip));
        System.out.println(Arrays.toString(getInternalHostsIpv4()));
    }

}

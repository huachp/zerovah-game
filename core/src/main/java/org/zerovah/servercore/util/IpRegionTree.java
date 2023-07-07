package org.zerovah.servercore.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * IP地区信息, 二叉树
 *
 * @author huachp
 */
public class IpRegionTree {

    private static final Logger LOGGER = LogManager.getLogger(IpRegionTree.class);

    private static final IpRegionTree EMPTY = new IpRegionTree();

    private IpNode rootNode = new IpNode();

    private void train(String ipStart, String ipEnd, int regionGroup) {
        int ipStartCode = ipToInt(ipStart);
        int ipEndCode = ipToInt(ipEnd);

        if (ipEndCode == -1 || ipStartCode == -1) {
            return;
        }
        IpNode curNode = rootNode;
        IpNode leftNode = null;
        IpNode rightNode = null;
        boolean flag = false;

        for (int i = 0; i < 32; i++) {
            int ipStartBit = (0x80000000 & ipStartCode) >>> 31;
            int ipEndBit = (0x80000000 & ipEndCode) >>> 31;
            if (!flag) {
                if ((ipStartBit ^ ipEndBit) == 0) {
                    if (ipStartBit == 1) {
                        if (curNode.rightNode == null) {
                            curNode.rightNode = new IpNode();
                        }
                        curNode = curNode.rightNode;
                    } else {
                        if (curNode.leftNode == null) {
                            curNode.leftNode = new IpNode();
                        }
                        curNode = curNode.leftNode;
                    }
                    if (i == 31) {
                        curNode.regionCode = (byte) regionGroup;
                    }
                } else {
                    flag = true;
                    if (curNode.leftNode == null) {
                        curNode.leftNode = new IpNode();
                    }
                    leftNode = curNode.leftNode;

                    if (curNode.rightNode == null) {
                        curNode.rightNode = new IpNode();
                    }
                    rightNode = curNode.rightNode;

                    if (i == 31) {
                        leftNode.regionCode = (byte) regionGroup;
                        rightNode.regionCode = (byte) regionGroup;
                    }
                }
            } else {
                if (ipStartBit == 1) {
                    if (leftNode.rightNode == null) {
                        leftNode.rightNode = new IpNode();
                    }
                    leftNode = leftNode.rightNode;
                } else {
                    if (leftNode.leftNode == null) {
                        leftNode.leftNode = new IpNode();
                    }
                    if (leftNode.rightNode == null) {
                        leftNode.rightNode = new IpNode();
                    }
                    leftNode.rightNode.regionCode = (byte) regionGroup;
                    leftNode = leftNode.leftNode;
                }
                if (i == 31) {
                    leftNode.regionCode = (byte) regionGroup;
                }
                if (ipEndBit == 1) {
                    if (rightNode.rightNode == null) {
                        rightNode.rightNode = new IpNode();
                    }
                    if (rightNode.leftNode == null) {
                        rightNode.leftNode = new IpNode();
                    }
                    rightNode.leftNode.regionCode = (byte) regionGroup;
                    rightNode = rightNode.rightNode;
                } else {
                    if (rightNode.leftNode == null) {
                        rightNode.leftNode = new IpNode();
                    }
                    rightNode = rightNode.leftNode;
                }
                if (i == 31) {
                    rightNode.regionCode = (byte) regionGroup;
                }
            }

            ipStartCode = ipStartCode << 1;
            ipEndCode = ipEndCode << 1;
        }
    }

    private int ipToInt(String strIP) {
        try {
            int[] ip = new int[4];

            int position1 = strIP.indexOf(".");
            int position2 = strIP.indexOf(".", position1 + 1);
            int position3 = strIP.indexOf(".", position2 + 1);

            ip[0] = Integer.parseInt(strIP.substring(0, position1));
            ip[1] = Integer.parseInt(strIP.substring(position1 + 1, position2));
            ip[2] = Integer.parseInt(strIP.substring(position2 + 1, position3));
            ip[3] = Integer.parseInt(strIP.substring(position3 + 1));

            return (ip[0] << 24) | (ip[1] << 16) | (ip[2] << 8) | ip[3];
        } catch (Exception e) {
            LOGGER.error("ip[{}]不合法", strIP);
            return -1;
        }
    }

    public int findIp(String ip) {
        IpNode curNode = rootNode;
        int ipCode = ipToInt(ip);
        if (ipCode == -1) {
            return -1;
        }
        for (int i = 0; i < 32; i++) {
            int ipStartBit = (0x80000000 & ipCode) >>> 31;
            if (ipStartBit == 0)
                curNode = curNode.leftNode;
            else
                curNode = curNode.rightNode;

            if (curNode == null) {
                return -1;
            }
            if (curNode.regionCode != 0) {
                return curNode.regionCode & 0xFF;
            }

            ipCode = ipCode << 1;
        }
        return -1;
    }


    private static class IpNode {

        private IpNode leftNode;
        private IpNode rightNode;
        private byte regionCode;
    }


//    private static void getIpLocationFromDb(CompletableFuture<Object> dbFuture, List data) {
//        List<IpLocation> ipLocationList = data;
//        dbFuture.thenAccept(t -> {
//            Map<Integer, IpRegion> ipRegionMap = (Map) t;
//            IpRegionTree tree = new IpRegionTree();
//            for (IpLocation ipLocation : ipLocationList) {
//                Integer geographyId = ipLocation.getGeographyId();
//                IpRegion ipRegion = ipRegionMap.get(geographyId);
//                tree.train(ipLocation.getIpStart(), ipLocation.getIpEnd(), ipRegion.getRegionGroup());
//            }
//        });
//    }
//
//    private static void getIpRegionFromDb(CompletableFuture<Object> future, List data) {
//        List<IpRegion> ipRegionList = data;
//        Map<Integer, IpRegion> ipRegionMap = new HashMap<>(512);
//        for (IpRegion ipRegion : ipRegionList) {
//            ipRegionMap.put(ipRegion.getGeographyId(), ipRegion);
//        }
//        future.complete(ipRegionMap);
//    }

}

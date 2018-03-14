package org.ansu.HDFS;
import org.apache.hadoop.net.DNSToSwitchMapping;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义机架感知类
 */
public class rackAware implements DNSToSwitchMapping {
    /**
     * 解析主机名/IP地址
     * @param list
     * @return
     */
    @Override
    public List<String> resolve(List<String> list) {
        List<String> paths = new ArrayList<String>();
        // 判断集合有效性
        if (list != null && list.isEmpty()){

            for (String hostname:list){
                Integer no = Integer.parseInt(hostname.substring(1));
                // 主机名s100-s800
                String rackPath = null;
                if (no <= 400){
                    rackPath = "/rack1/"+hostname;
                }else{
                    rackPath = "/rack2/"+hostname;
                }
                paths.add(rackPath);
            }
        }
        return paths;
    }
    @Override
    public void reloadCachedMappings() { }
    @Override
    public void reloadCachedMappings(List<String> list) { }
}

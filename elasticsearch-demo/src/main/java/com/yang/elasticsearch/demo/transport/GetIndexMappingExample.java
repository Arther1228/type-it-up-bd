package com.yang.elasticsearch.demo.transport;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *  tanrsport client 版本 6.4.3 可以访问 Windows 6.5.4 测试通过
 */
public class GetIndexMappingExample {

    public static void main(String[] args) throws UnknownHostException {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                //              .addTransportAddress(new TransportAddress(InetAddress.getByName("172.16.99.5"), 9300));
               .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

        GetMappingsRequest request = new GetMappingsRequest();
        request.indices("user-info");

        GetMappingsResponse response = client.admin().indices().getMappings(request).actionGet();
        ImmutableOpenMap<String, MappingMetaData> mappings = response.mappings().get("user-info");
        for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
            System.out.println(cursor.value.sourceAsMap());
        }

        client.close();

    }
}

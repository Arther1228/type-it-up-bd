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
 * //TODO 测试未通过
 */
public class GetIndexMappingExample {

    public static void main(String[] args) throws UnknownHostException {

        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("34.8.8.122"), 24001));

        GetMappingsRequest request = new GetMappingsRequest();
        request.indices("my_index2");

        GetMappingsResponse response = client.admin().indices().getMappings(request).actionGet();
        ImmutableOpenMap<String, MappingMetaData> mappings = response.mappings().get("my_index2");
        for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
            System.out.println(cursor.value.sourceAsMap());
        }

        client.close();

    }
}

package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelSearchTest {
    private RestHighLevelClient client;

    @BeforeEach
    void beforeAll() {
        this.client= new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.88.130:9200")
        ));
    }

    @AfterEach
    void afterAll() throws IOException {
        this.client.close();
    }

    /**
     * 查询所有
     * @throws IOException
     */
    @Test
    void testMatchAll() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.matchAllQuery());
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        // 4.解析结果
        handleResponse(response);

    }

    /**
     * 精确查询
     * @throws IOException
     */
    @Test
    void testMatch() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        request.source().query(QueryBuilders.matchQuery("name","如家"));
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

//        System.out.println(response);

        handleResponse(response);

    }


    /**
     * 精确查询
     * @throws IOException
     */
    @Test
    void testBool() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        // 2.1.准备BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // 2.2.添加term
        boolQuery.must(QueryBuilders.termQuery("city","北京"));
        // 2.3.添加range
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));
        // 2.4.添加match
        request.source().query(boolQuery);
        // 3.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }


    @Test
    void testPageAndSort() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        // 2.1.准备分页条件
        request.source().from(0).size(5);

        // 3.价格排序
        request.source().sort("price", SortOrder.ASC);

        // 4.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    @Test
    void testHighlight() throws IOException {
        // 1.准备Request
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DSL
        // 2.1.准备分页条件
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        // 2.2.高亮
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 3.价格排序
        request.source().sort("price", SortOrder.ASC);
        // 4.发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    private static void handleResponse(SearchResponse response) {
        // 4.解析结果
        SearchHits searchHits = response.getHits();
        // 4.1.查询的总条数并打印
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到" + total + "条数据");
        // 4.2查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        Stream.of(hits).forEach(i-> {
            String json = i.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Map<String, HighlightField> highlightFields = i.getHighlightFields();
            if (!CollectionUtils.isEmpty(highlightFields)){
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null) {
                    String name = highlightField.getFragments()[0].string();
                    hotelDoc.setName(name);
                }
            }
            System.out.println("hotelDoc = " + hotelDoc);
        });
    }





}

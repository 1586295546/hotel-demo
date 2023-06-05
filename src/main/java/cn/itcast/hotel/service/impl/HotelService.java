package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1.准备Request
            SearchRequest request = new SearchRequest("hotel");
            // 2.准备DSL
            // 2.1 准备查询条件query
            // 构建BooleanQuery
            buildBasicQuery(params, request);
            // 2.2. 准备分页条件
            int page = params.getPage();
            int size = params.getSize();
            int from = (page - 1) * size;
            request.source().from(from).size(size);

            // 2.3.排序
            String location = params.getLocation();
            if (location != null && !location.equals("")){
                request.source().sort(SortBuilders
                        .geoDistanceSort("location",new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            // 3.发送请求, 得到响应
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4.解析响应, 返回结果
            return handleResponse(response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static BoolQueryBuilder buildBasicQuery(RequestParams params, SearchRequest request) {
        // 构建BooleanQuery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // must部分:关键字搜索
        String key = params.getKey();
        if (key == null || key.trim().length() == 0) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("name", key));
        }
        // 城市条件
        if (params.getCity() != null && !params.getCity().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }
        // 品牌条件
        if (params.getBrand() != null && !params.getBrand().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }
        // 星级条件
        if (params.getStarName() != null && !params.getStarName().equals("")){
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }
        // 价格
        if (params.getMinPrice() != null && params.getMaxPrice() != null){
            boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }

        // 2.算分控制
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(
                        // 原始查询, 相关性算分的查询
                        boolQuery,
                        // function score 的数组
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 其中的一个function score 元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        // 过滤条件
                                        QueryBuilders.termQuery("isAD", true),
                                        // 算分规则: 权重分*10
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                ),
                        });

        request.source().query(functionScoreQueryBuilder);

        return boolQuery;
    }

    private static PageResult handleResponse(SearchResponse response) {
        // 4.解析结果
        SearchHits searchHits = response.getHits();
        // 4.1.查询的总条数并打印
        long total = searchHits.getTotalHits().value;
        // 4.2查询的结果数组
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotelDocs = new ArrayList<>();
        Stream.of(hits).forEach(i -> {
            // 4.3.获取_source字段的值
            String json = i.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            hotelDocs.add(hotelDoc);
            // 获取排序值
            Object[] sortValues = i.getSortValues();
            if (sortValues != null && sortValues.length > 0){
                Object sortValue = sortValues[0];
                hotelDoc.setDistance(sortValue);
            }
        });
        return new PageResult(total, hotelDocs);
    }
}

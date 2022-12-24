package com.kv.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kv.hero.dto.HeroesDto;
import com.kv.misc.dto.Container;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.function.Supplier;

@Service
public class Dota2QueryService {

    private static final String DOTA2_HEROES_LIST_SERVICE_URL = "https://www.dota2.com/datafeed/herolist?language=english";

    private static List<HeroesDto> cachedHeroesListData = null;

    @Autowired
    private RestTemplate restTemplate;

    private List<HeroesDto> getDota2HeroesNameList() {
        List<HeroesDto> list = new ArrayList<>();

        if(cachedHeroesListData != null) {
            return cachedHeroesListData;
        } else {
            ObjectMapper objectMapper = new ObjectMapper();
            ResponseEntity<Map<String, Object>> responseEntity = restTemplate
                    .exchange(DOTA2_HEROES_LIST_SERVICE_URL, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});
            if(responseEntity.hasBody()) {
                Map<String, Object> result = (Map) responseEntity.getBody().get("result");
                Map<String, Object> data = (Map) result.get("data");
                list = objectMapper.convertValue(data.get("heroes"), new TypeReference<>() {
                });
                cachedHeroesListData = list;
            }
        }
        return list;
    }

    public Supplier<List<HeroesDto>> dota2AllHeroesListSupplier = () -> getDota2HeroesNameList();
}

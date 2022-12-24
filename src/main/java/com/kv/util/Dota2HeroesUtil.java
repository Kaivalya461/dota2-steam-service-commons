package com.kv.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kv.hero.dto.HeroesDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface Dota2HeroesUtil {
    Logger log = LoggerFactory.getLogger(Dota2HeroesUtil.class);

    String HERO_LIST_JSON_FILE = "utils/heroinfo.json";

    /**
     * Get Dota2 Hero name for given HeroID. Reads a json file containing all HeroIDs and their equivalent Hero names.
     * @param heroId It is a numeric ID for every Dota2 hero usually in the range of 1 to 135 as of patch 7.29.
     * @return Hero's localized name in String format.
     */
    static String getHeroName(Integer heroId) throws Exception {
        List<HeroesDto> heroesDtoList = new ArrayList<>();
        HeroesDto heroDto = null;
        String heroName = null;

//        try {
//            File heroInfoJsonFile = new File(Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource(jsonFileLocation)).toURI());
//            Map<String, Object> heroesInfo = objectMapper.readValue(heroInfoJsonFile, new TypeReference<>() {});

            heroesDtoList = Dota2HeroesUtil.getAllHeroes();
//        } catch (URISyntaxException | IOException e) {
//            log.error("Exception occurred while reading/converting HeroInfo json file");
//            e.printStackTrace();
//        }

        try {
            heroDto = heroesDtoList.stream().filter(x -> x.getId() == (heroId)).findFirst().get();
            heroName = heroDto.getLocalized_name();
        } catch (Exception e) {
            log.info("Hero not Found for heroId: {}", heroId);
            log.error(e.getMessage());
            throw new Exception("Hero not found for heroID: " + heroId);
        }

        return heroName;
    }

    static List<HeroesDto> getAllHeroes() {
        List<HeroesDto> heroesDtoList = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        InputStream heroListJsonFileInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(HERO_LIST_JSON_FILE);
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(heroListJsonFileInputStream));

            Map<String, Object> heroesInfo = objectMapper.readValue(bufferedReader, new TypeReference<Map<String, Object>>() {});

            heroesDtoList = objectMapper.convertValue(heroesInfo.get("heroes"), new TypeReference<List<HeroesDto>>() {});
        } catch (IOException e) {
            log.error("Exception occurred while reading/converting HeroInfo json file");
            e.printStackTrace();
        } catch (Exception ex) {
            log.error("Exception occurred while reading/converting HeroInfo json file with error message:  {}", ex.getMessage() ,ex);
        }

        return heroesDtoList;
    }


}

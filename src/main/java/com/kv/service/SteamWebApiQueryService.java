package com.kv.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kv.constants.Dota2SteamWebApiUrls;
import com.kv.constants.SteamWebApiKeys;
import com.kv.dao.MatchDetailsDaoFileStorageImpl;
import com.kv.matchdetails.dto.MatchDetailsDto;
import com.kv.matchdetails.dto.MatchHistoryDto;
import com.kv.matchdetails.dto.MatchesDto;
import com.kv.misc.dto.MatchDetailsContainer;
import com.kv.misc.dto.SteamWebApiResponseContainer;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Log4j2
public class SteamWebApiQueryService {

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MatchDetailsDaoFileStorageImpl matchDetailsDaoFileStorage;

    //CacheData
    private static final Map<String, MatchDetailsDto> cacheForMatchDetailsAPI = new HashMap<>();

    private static final short ONE_HOUR_TIME_IN_SECONDS = 3600;
    private static final short THREE_HOURS_TIME_IN_SECONDS = ONE_HOUR_TIME_IN_SECONDS * 3;
    private static final short FIVE_AND_HALF_HOURS_TIME_IN_SECONDS = 19800;

    /**
     * <h3>Steam Web API -> GetMatchHistory <- </h3>
     * <a href="https://wiki.teamfortress.com/wiki/WebAPI/GetMatchHistory">API Doc:</a>
     * <p>This function does a rest call to steam web api and provides MatchHistory details for given Dota2 account ID.</p>
     */
    public MatchHistoryDto getMatchHistory(String dota2AccountId, String startAtMatchId, String heroId) {

        MatchHistoryDto matchHistoryDto = new MatchHistoryDto();

        String matchHistoryUrl = buildMatchHistoryUrl(dota2AccountId, startAtMatchId, heroId);
        ResponseEntity<Map<String, Object>> response = restTemplate.exchange(matchHistoryUrl, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});

        try {
            String responseString = null;
            responseString = objectMapper.writeValueAsString(Objects.requireNonNull(response.getBody()).get("result"));
            matchHistoryDto = objectMapper.readValue(responseString, MatchHistoryDto.class);
//            Map<String, Object> map = objectMapper.readValue(responseString, new TypeReference<>() {});
//            matchHistoryDto = objectMapper.convertValue(map, MatchHistoryDto.class);
        } catch (JsonProcessingException e) {
            log.error("Error occurred while converting json response to Java model - {}", e.getMessage());
            e.printStackTrace();
        }

        return matchHistoryDto;
    }

    /**
     * <p>
     *     Since May 2024 - Steam Web API "GetMatchDetails" has stopped working. We are just getting 500 HttpResponse with empty response body.
     *     Alternative for this is to migrate to Steam Web API "GetMatchHistoryBySequenceNum"
     * </p>
     * <ul>
     *     Issue Tickets:
     *     <li><a href="https://github.com/ValveSoftware/Dota-2/issues/2715">Ticket 1</a></li>
     *     <li><a href="https://github.com/ValveSoftware/Dota2-Gameplay/issues/17910"> Ticket 2</a></li>
     * </ul>
     *
     * <h3>Steam Web API -> GetMatchDetails <- </h3>
     * <a href="https://wiki.teamfortress.com/wiki/WebAPI/GetMatchDetails">API Doc:</a>
     * <p>This function does a rest call to steam web api and provides Match Details for given Dota2 account ID and Match Id.</p>
     */
    public MatchDetailsDto getMatchDetails(String matchId, Optional<Long> matchSeqNumOpt) {
        MatchDetailsDto matchDetails = new MatchDetailsDto();
        String url = buildMatchDetailsUrl(matchId);
        ResponseEntity<MatchDetailsContainer> matchDetailsAPIResponseEntity = null;

        if(cacheForMatchDetailsAPI.containsKey(matchId)) {
            log.info("Cache return for matchId -> {}", matchId);
            return cacheForMatchDetailsAPI.get(matchId);
        }

        //get data from local db
        if (matchDetailsDaoFileStorage.dataSourceEnabled) {
            var matchDetailFromDB = getDataFromDb(matchId);
            if(Objects.nonNull(matchDetailFromDB) && matchDetailFromDB.getMatch_id() == Long.parseLong(matchId)) {
                log.info("Local DB return for matchId -> {}", matchId);
                return matchDetailFromDB;
            }
        }

        try {
            matchDetailsAPIResponseEntity = restTemplate.exchange(
                    url, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});
            if(matchDetailsAPIResponseEntity.hasBody()) {
                matchDetails = matchDetailsAPIResponseEntity.getBody().getResult();
            }
        } catch (HttpServerErrorException httpServerErrorException) {
            log.error("HttpServerErrorException -> steam web api for matchId: {}", matchId, httpServerErrorException);
            matchDetailsAPIResponseEntity = ResponseEntity.status(httpServerErrorException.getStatusCode()).build();
        } catch (Exception e) {
            log.error("Error occurred while fetching data from steam web api for matchId: {}", matchId, e);
        }

        //Temp fix to avoid migration of all APIs to GetMatchHistoryBySequenceNum
        //fall back to GetMatchHistoryBySequenceNum in case GetMatchDetails API fails with 500 response(As; since May 2024 API has stopped working)
        if(Objects.nonNull(matchDetailsAPIResponseEntity) && matchDetailsAPIResponseEntity.getStatusCode().is5xxServerError() && matchSeqNumOpt.isPresent()) {
            log.warn("Falling back to GetMatchHistoryBySequenceNum");
            Optional<SteamWebApiResponseContainer<MatchHistoryDto>> matchHistoryResponseOpt = getMatchHistoryBySequenceNum(matchSeqNumOpt, Optional.of(1));
            MatchDetailsDto finalMatchDetails = matchDetails;
            matchHistoryResponseOpt
                    .map(SteamWebApiResponseContainer::getResult)
                    .map(MatchHistoryDto::getMatches)
                    .ifPresent(matches -> {
                        if (!CollectionUtils.isEmpty(matches)) {
                            BeanUtils.copyProperties(matches.get(0), finalMatchDetails);
                        }
                    });
        }

        //Cache-Save and save to local DB
        cacheForMatchDetailsAPI.put(String.valueOf(matchDetails.getMatch_id()), matchDetails);
        if(matchDetailsDaoFileStorage.dataSourceEnabled)
            saveDataToDB(matchDetails);

        return matchDetails;
    }

    /**
     * <h3>Steam Web API -> GetMatchHistoryBySequenceNum <- </h3>
     * <a href="https://wiki.teamfortress.com/wiki/WebAPI/GetMatchHistoryBySequenceNum">API Doc: </a>
     * <p>This function does a rest call to steam web api and provides MatchHistory details for given Dota2 account ID.</p>
     */
    public Optional<SteamWebApiResponseContainer<MatchHistoryDto>> getMatchHistoryBySequenceNum(Optional<Long> startAtMatchSeqNumOpt, Optional<Integer> matchesRequestedOpt) {
        String matchHistoryUrl = buildMatchHistoryBySequenceNumUrl(startAtMatchSeqNumOpt, matchesRequestedOpt);
        try {
            return restTemplate.exchange(
                            matchHistoryUrl,
                            HttpMethod.GET,
                            null, new ParameterizedTypeReference<Optional<SteamWebApiResponseContainer<MatchHistoryDto>>>() {}
                    ).getBody();
        } catch (Exception ex) {
            log.error("Error in SteamWebApiQueryService::getMatchHistoryBySequenceNum -> ErrorMessage: {}, GET URL: {}, ErrorStackTrace: {}",
                    ex.getMessage(), matchHistoryUrl, ex.getStackTrace());
            return Optional.empty();
        }
    }

    /**
     * Overloaded method for "GetMatchDetails" to keep backwards compatibility for other services.
     */
    public MatchDetailsDto getMatchDetails(String matchId) {
        return getMatchDetails(matchId, Optional.empty());
    }

    /**
     * This function will return last 500 Matches for given dota2 account Id.
     */
    public Set<MatchesDto> getLast500MatchesForDota2AccountId(String dota2AccountId) throws Exception {
        MatchHistoryDto matchHistoryDto = getMatchHistory(dota2AccountId, null, null);
        Set<MatchesDto> matchesDtoSet = new HashSet<>();
        if(containsMatchesPlayed(matchHistoryDto))
             matchesDtoSet = new HashSet<>(matchHistoryDto.getMatches());
        else if(matchHistoryDto.getStatusDetail() != null) {
            throw new Exception(matchHistoryDto.getStatusDetail());
        }

        while(matchHistoryDto.getStatus() == 1 && matchHistoryDto.getResults_remaining() != 0) {
            long startAtMatchId = Objects.requireNonNull(matchHistoryDto.getMatches().stream().skip(matchHistoryDto.getMatches().size()-1).findFirst().orElse(null)).getMatch_id();
            matchHistoryDto = getMatchHistory(dota2AccountId, startAtMatchId + "", null);
            matchesDtoSet.addAll(matchHistoryDto.getMatches());
        }

        return matchesDtoSet;
    }

    /**
     * This function will return last 500 Matches for given dota2 account Id and for a specific Dota2 hero id only.
     */
    //In-progress
    public Set<MatchesDto> getLast500MatchesForDota2AccountId(String dota2AccountId, String heroId) throws Exception {
        MatchHistoryDto matchHistoryDto = getMatchHistory(dota2AccountId, null, heroId);
        Set<MatchesDto> matchesDtoSet = new HashSet<>();
        if(containsMatchesPlayed(matchHistoryDto))
            matchesDtoSet = new HashSet<>(matchHistoryDto.getMatches());
        else if(matchHistoryDto.getStatusDetail() != null) {
            throw new Exception(matchHistoryDto.getStatusDetail());
        }

        while(matchHistoryDto.getStatus() == 1 && matchHistoryDto.getResults_remaining() != 0) {
            long startAtMatchId = Objects.requireNonNull(matchHistoryDto.getMatches().stream().skip(matchHistoryDto.getMatches().size()-1).findFirst().orElse(null)).getMatch_id();
            matchHistoryDto = getMatchHistory(dota2AccountId, startAtMatchId + "", null);
            matchesDtoSet.addAll(matchHistoryDto.getMatches());
        }

        return matchesDtoSet;
    }

    /**
     * This function will return all today's played Matches for given dota2 account Id. (Time Period is 0300 to 2400 plus next day's +3 hours)
     * Note: If player not played any matches today then it will return matches for the day which player last played.
     */
    public Set<MatchesDto> getTodaysPlayedMatches(String dota2AccountId) throws Exception {
        Set<MatchesDto> matchesDtoSet = new HashSet<>();
        List<MatchesDto> filteredMatches = new ArrayList<>();
        LocalDateTime latestMatchPlayedDate = null;

        MatchHistoryDto matchHistory = getMatchHistory(dota2AccountId, null, null);
        MatchesDto latestMatchPlayed = new MatchesDto();

        if(containsMatchesPlayed(matchHistory))
            latestMatchPlayed = matchHistory.getMatches().get(0);
        else if(matchHistory.getStatusDetail() != null)
            throw new Exception(matchHistory.getStatusDetail());


        //Finding the recent date for which the player has played some matches.
        //All Matches played after 3AM of this date are taken into processing.
        if(latestMatchPlayed != null) {
            latestMatchPlayedDate = LocalDateTime.ofEpochSecond(
                    latestMatchPlayed.getStart_time(), 0, ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS));  //Converting epoch time to IST LocalDateTime

            if(latestMatchPlayedDate.toLocalTime().isBefore(LocalTime.ofSecondOfDay(THREE_HOURS_TIME_IN_SECONDS)))
                latestMatchPlayedDate = latestMatchPlayedDate.minusDays(1); //Moving date 1 day in past as we mostly play some matches after 12am for that day...only if last match is played between 12am to 3am

            latestMatchPlayedDate = LocalDateTime.of(latestMatchPlayedDate.toLocalDate(), LocalTime.ofSecondOfDay(THREE_HOURS_TIME_IN_SECONDS));
        }

        //Passing above calculated DateTime, and filtering all the matches which are played after this time.
        if(latestMatchPlayedDate != null) {
            filteredMatches = getMatchesPlayedOfTheDay(latestMatchPlayedDate, matchHistory.getMatches());
            log.info("Total Matches played for that day({}): {}", latestMatchPlayedDate.toLocalDate(), filteredMatches.size());
        }

        //Logging date/time details for first and last matches of the day
        if(!filteredMatches.isEmpty())
            logDateTimeDetailsForFirstAndLastMatches(filteredMatches);

        matchesDtoSet = new HashSet<>(filteredMatches);

        return matchesDtoSet;
    }

    public Set<MatchesDto> getMatchesForDota2AccountId(String dota2AccountId, int noOfDaysMatches) throws Exception {
        MatchHistoryDto matchHistoryDto = getMatchHistory(dota2AccountId, null, null);
        Set<MatchesDto> matchesDtoSet = new HashSet<>();
        Collection<MatchesDto> tempMatchesDtoSet = new HashSet<>();
        if(containsMatchesPlayed(matchHistoryDto)) {
            tempMatchesDtoSet = new HashSet<>(matchHistoryDto.getMatches());
            matchesDtoSet.addAll(tempMatchesDtoSet);
        }
        else if(matchHistoryDto.getStatusDetail() != null) {
            throw new Exception(matchHistoryDto.getStatusDetail());
        }

        while(matchHistoryDto.getStatus() == 1 && matchHistoryDto.getResults_remaining() != 0 &&
                tempMatchesDtoSet.stream().anyMatch(x -> isMatchPlayedWithinLastNDays.test(x.getStart_time(), noOfDaysMatches))) {
            long startAtMatchId = Objects.requireNonNull(matchHistoryDto.getMatches().stream().skip(matchHistoryDto.getMatches().size()-1).findFirst().orElse(null)).getMatch_id();
            matchHistoryDto = getMatchHistory(dota2AccountId, startAtMatchId + "", null);
            tempMatchesDtoSet = matchHistoryDto.getMatches();
            matchesDtoSet.addAll(tempMatchesDtoSet);
        }

        return matchesDtoSet;
    }

    //In-progress
    public List<MatchDetailsDto> getMatchDetailsForTodaysMatches(Set<Long> matchIds) {
        return matchIds.stream()
                .map(matchId -> getMatchDetails(String.valueOf(matchId)))
                .collect(Collectors.toList());
    }

    private void logDateTimeDetailsForFirstAndLastMatches(List<MatchesDto> matchesDtoList) {
        Consumer<List<MatchesDto>> logFirstAndLastMatchPlayedDateDetails = matches -> {
            //converting epoch time to IST LocalDateTime
            LocalDateTime firstMatchOfTheDayTime = LocalDateTime.ofEpochSecond(matches.stream().map(MatchesDto::getStart_time).min(Long::compare).get(), 0, ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS));
            LocalDateTime lastMatchOfTheDayTime = LocalDateTime.ofEpochSecond(matches.stream().map(MatchesDto::getStart_time).max(Long::compare).get(), 0, ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS));

            if(firstMatchOfTheDayTime.equals(lastMatchOfTheDayTime)) {
                log.info("Only 1 match played bruh.. {}", firstMatchOfTheDayTime);
            }
            log.info("First Match of the day was played at {}", firstMatchOfTheDayTime);
            log.info("Last Match of the day was played at {}", lastMatchOfTheDayTime);
        };

        logFirstAndLastMatchPlayedDateDetails.accept(matchesDtoList);
    }

    /**
     * This function will return all the matches who start time is greater than requestedDate.
     */
    private List<MatchesDto> getMatchesPlayedOfTheDay(LocalDateTime requestedDate, List<MatchesDto> allMatches) {
        return allMatches.stream().filter(x -> x.getStart_time() >= requestedDate.toEpochSecond(ZoneOffset.ofTotalSeconds(FIVE_AND_HALF_HOURS_TIME_IN_SECONDS))).collect(Collectors.toList());
    }

    private boolean containsMatchesPlayed(MatchHistoryDto matchHistoryDto) {
        Predicate<MatchHistoryDto> predicate = x -> x.getStatus() == 1 && x.getMatches() != null;
        return predicate.test(matchHistoryDto);
    }


    /**
     * This function builds url for MatchHistory steam web api
     */
    private String buildMatchHistoryUrl(String dota2AccountId, String startAtMatchId, String heroId) {
        UriComponentsBuilder uriComponentsBuilder = UriComponentsBuilder.fromHttpUrl(Dota2SteamWebApiUrls.MATCH_HISTORY_URL)
                .queryParam("key", SteamWebApiKeys.PRIMARY_STEAM_ACCOUNT_WEB_API_KEY)
                .queryParam("account_id", dota2AccountId);

        if(startAtMatchId != null && !startAtMatchId.isEmpty())
            uriComponentsBuilder.queryParam("start_at_match_id", startAtMatchId);
        if(heroId != null && !heroId.isEmpty())
            uriComponentsBuilder.queryParam("hero_id", heroId);

        return uriComponentsBuilder.toUriString();
    }

    /**
     * This function builds url for MatchDetails steam web api
     */
    private String buildMatchDetailsUrl(String matchId) {
        UriComponentsBuilder uriComponentsBuilder = UriComponentsBuilder.fromHttpUrl(Dota2SteamWebApiUrls.MATCH_DETAILS_URL)
                .queryParam("key", SteamWebApiKeys.PRIMARY_STEAM_ACCOUNT_WEB_API_KEY)
                .queryParam("match_id", matchId);
        return uriComponentsBuilder.toUriString();
    }

    /**
     * This function builds url for GetMatchHistoryBySequenceNum steam web api
     */
    private String buildMatchHistoryBySequenceNumUrl(Optional<Long> startAtMatchSeqNumOpt, Optional<Integer> numberOfMatchesRequestedOpt) {
        UriComponentsBuilder uriComponentsBuilder = UriComponentsBuilder.fromHttpUrl(Dota2SteamWebApiUrls.MATCH_HISTORY_BY_SEQUENCE_NUM_URL)
                .queryParam("key", SteamWebApiKeys.PRIMARY_STEAM_ACCOUNT_WEB_API_KEY)
                .queryParamIfPresent("start_at_match_seq_num", startAtMatchSeqNumOpt)
                .queryParamIfPresent("matches_requested", numberOfMatchesRequestedOpt);
        return uriComponentsBuilder.toUriString();
    }

    private MatchDetailsDto getDataFromDb(String matchId) {
        return matchDetailsDaoFileStorage.findByMatchId(matchId);
    }

    private void saveDataToDB(MatchDetailsDto matchDetails) {
        matchDetailsDaoFileStorage.save(matchDetails);
    }

    Predicate<Long> isMatchPlayedWithinLast30Days = (matchStartTime) -> LocalDate.ofInstant(Instant.ofEpochSecond(matchStartTime), ZoneId.of("Asia/Kolkata")).isAfter(LocalDate.now().minusDays(30));
    BiPredicate<Long, Integer> isMatchPlayedWithinLastNDays = (matchStartTime, numberOfDays)
            -> LocalDate.ofInstant(Instant.ofEpochSecond(matchStartTime), ZoneId.of("Asia/Kolkata")).isAfter(LocalDate.now().minusDays(numberOfDays));

}

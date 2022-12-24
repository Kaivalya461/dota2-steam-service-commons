package com.kv.dao;

import com.kv.matchdetails.dto.MatchDetailsDto;

import java.util.Collection;

public interface MatchDetailsDao<T> {
    Collection<T> findByMatchIds(Collection<T> matchIds);

    T findByMatchId(Object matchId);

    T save(T matchDetailsDto);
}

package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface ConsolidatedVideoRatingDelegate extends HollowObjectDelegate {

    public int getCountryRatingsOrdinal(int ordinal);

    public int getCountryListOrdinal(int ordinal);

    public ConsolidatedVideoRatingTypeAPI getTypeAPI();

}
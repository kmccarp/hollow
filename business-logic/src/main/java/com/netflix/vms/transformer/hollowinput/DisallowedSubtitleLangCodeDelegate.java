package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.objects.delegate.HollowObjectDelegate;


@SuppressWarnings("all")
public interface DisallowedSubtitleLangCodeDelegate extends HollowObjectDelegate {

    public int getValueOrdinal(int ordinal);

    public DisallowedSubtitleLangCodeTypeAPI getTypeAPI();

}
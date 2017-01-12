package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.objects.HollowObject;

@SuppressWarnings("all")
public class VideoGeneralTitleTypeHollow extends HollowObject {

    public VideoGeneralTitleTypeHollow(VideoGeneralTitleTypeDelegate delegate, int ordinal) {
        super(delegate, ordinal);
    }

    public StringHollow _getValue() {
        int refOrdinal = delegate().getValueOrdinal(ordinal);
        if(refOrdinal == -1)
            return null;
        return  api().getStringHollow(refOrdinal);
    }

    public VMSHollowInputAPI api() {
        return typeApi().getAPI();
    }

    public VideoGeneralTitleTypeTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected VideoGeneralTitleTypeDelegate delegate() {
        return (VideoGeneralTitleTypeDelegate)delegate;
    }

}
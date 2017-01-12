package com.netflix.vms.transformer.hollowinput;

import com.netflix.hollow.api.objects.HollowObject;

@SuppressWarnings("all")
public class TranslatedTextValueHollow extends HollowObject {

    public TranslatedTextValueHollow(TranslatedTextValueDelegate delegate, int ordinal) {
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

    public TranslatedTextValueTypeAPI typeApi() {
        return delegate().getTypeAPI();
    }

    protected TranslatedTextValueDelegate delegate() {
        return (TranslatedTextValueDelegate)delegate;
    }

}
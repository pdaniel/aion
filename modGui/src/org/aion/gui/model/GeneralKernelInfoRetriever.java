package org.aion.gui.model;

import org.aion.api.type.ApiMsg;
import org.aion.gui.model.dto.SyncInfoDto;
import org.aion.log.AionLoggerFactory;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Class contains methods for retrieving info from the API that is too simple to warrant
 * its own DTO class, i.e. {@link SyncInfoDto}.
 */
public class GeneralKernelInfoRetriever extends AbstractAionApiClient {
    private static final Logger LOG = AionLoggerFactory.getLogger(org.aion.log.LogEnum.GUI.name());

    public GeneralKernelInfoRetriever(KernelConnection kernelConnection) {
        super(kernelConnection);
    }

    public boolean isMining() throws ApiDataRetrievalException {
        ApiMsg resp = callApi(api -> api.getMine().isMining());
        throwAndLogIfError(resp);
        return (boolean)resp.getObject();
    }

    public int getPeerCount() throws ApiDataRetrievalException {
        ApiMsg resp = callApi(api -> api.getNet().getActiveNodes());
        throwAndLogIfError(resp);
        return ((List) resp.getObject()).size();
    }
}
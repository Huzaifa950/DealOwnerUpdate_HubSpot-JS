const fetch = require('node-fetch');
const moment = require('moment');

const apiKey = '';
const maxRetries = 60;
const maxPagesPerBatch = 100;
const batchSize = 100;

async function fetchDeals() {
    const dealsUrl = 'https://api.hubapi.com/crm/v3/objects/deals/search';
    const pipelineId = '8214425';
    const createdateStart = '2021-01-04T00:00:00Z';
    const createdateEnd = '2021-01-15T23:59:59Z';

    let allDeals = [];
    let page = 0;
    let after = null;
    let allResponses = [];

    try {
        while (page < maxPagesPerBatch) {
            const requestBody = {
                filterGroups: [
                    {
                        filters: [
                            {
                                propertyName: "createdate",
                                operator: "BETWEEN",
                                value: createdateStart,
                                highValue: createdateEnd
                            },
                            {
                                propertyName: "pipeline",
                                operator: "EQ",
                                value: pipelineId
                            }
                        ]
                    }
                ],
                properties: ["hubspot_owner_id", "createdate"],
                limit: 100,
                after: after
            };

            console.log(`Fetching deals from: ${dealsUrl}`);

            const response = await fetchWithRetry(dealsUrl, apiKey, maxRetries, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${apiKey}`
                },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();

            if (data && data.results) {
                allResponses.push(data);

                const dealsWithCompany = await Promise.all(data.results.map(async (deal) => {
                    const companyId = await fetchCompanyAssociation(deal.id, apiKey, maxRetries);
                    const createdAt = deal.properties.createdate;
                    const hubspotOwnerId = deal.properties.hubspot_owner_id;

                    let companyData = null;
                    if (companyId) {
                        companyData = await fetchCompanyData(companyId, apiKey, maxRetries);
                    }

                    return { ...deal, companyId, createdAt, hubspotOwnerId, companyData };
                }));

                allDeals = [...allDeals, ...dealsWithCompany];

                if (data.paging && data.paging.next) {
                    after = data.paging.next.after;
                } else {
                    break;
                }
            } else {
                console.error('No results found in the response');
                break;
            }

            page++;
        }

        // Print all deals
        console.log('Total number of deals:', allDeals.length);

        // Print all responses
        // allResponses.forEach((response, index) => {
        //     console.log(`Response from page ${index + 1}:`, response);
        // });

        return { allDeals, allResponses };
    } catch (error) {
        console.error('Error fetching deals:', error);
        return { allDeals, allResponses };
    }
}

async function fetchCompanyAssociation(dealId, apiKey, maxRetries) {
    const url = `https://api.hubapi.com/crm/v3/objects/deals/${dealId}/associations/companies`;

    try {
        const response = await fetchWithRetry(url, apiKey, maxRetries);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        if (data && data.results && data.results.length > 0) {
            return data.results[0].id;
        } else {
            return null;
        }
    } catch (error) {
        console.error(`Error fetching company association for deal ${dealId}:`, error);
        return null;
    }
}

async function fetchCompanyData(companyId, apiKey, maxRetries) {
    const url = 'https://api.hubapi.com/crm/v3/objects/companies/batch/read';

    try {
        const response = await fetchWithRetry(url, apiKey, maxRetries, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                "propertiesWithHistory": [
                    "hubspot_owner_id"
                ],
                "inputs": [
                    {
                        "id": companyId
                    }
                ]
            })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        return data.results[0];
    } catch (error) {
        console.error(`Error fetching company data for company ${companyId}:`, error);
        return null;
    }
}

async function updateDealOwner(dealId, newOwnerId, apiKey, maxRetries) {
    const url = `https://api.hubapi.com/crm/v3/objects/deals/${dealId}`;

    try {
        const response = await fetchWithRetry(url, apiKey, maxRetries, {
            method: 'PATCH',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                properties: {
                    hubspot_owner_id: newOwnerId
                }
            })
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        console.error(`Error updating deal ${dealId} owner:`, error);
        return null;
    }
}

async function fetchWithRetry(url, apiKey, maxRetries, options = {}) {
    let attempts = 0;
    const maxDelay = 32000;
    const retryCodes = [408, 429, 500, 502, 503, 504];

    while (attempts < maxRetries) {
        attempts++;
        const response = await fetch(url, {
            method: options.method || 'GET',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json',
                ...options.headers
            },
            body: options.body
        });

        if (!retryCodes.includes(response.status)) {
            return response;
        }

        const retryAfter = response.headers.get('Retry-After');
        let delay = retryAfter ? parseInt(retryAfter) * 1000 : Math.pow(1, attempts) * 1000;
        if (delay > maxDelay) {
            delay = maxDelay;
        }

        // console.log(`Retrying in ${delay / 1000} second...`);
        await new Promise(resolve => setTimeout(resolve, delay));
    }

    throw new Error('Max retries reached');
}



const processDealsBatch = async (after = 0) => {
    const { allDeals } = await fetchDeals(after);

    const filteredDeals = allDeals.reduce((filtered, deal, index) => {
        const companyDataArray = [];

        if (
            deal.companyData &&
            deal.companyData.propertiesWithHistory &&
            deal.companyData.propertiesWithHistory.hubspot_owner_id &&
            deal.companyData.propertiesWithHistory.hubspot_owner_id.length > 0
        ) {
            deal.companyData.propertiesWithHistory.hubspot_owner_id.forEach((owner, ownerIndex) => {
                const companyData = {
                    id: ownerIndex + 1,
                    value: owner.value,
                    timestamp: owner.timestamp
                };
                companyDataArray.push(companyData);
            });
        }

        if (companyDataArray.some(data => data.value !== null && data.timestamp !== null)) {
            let selectedCompanyData = null;
            const dealCreatedAt = moment(deal.createdAt);

            for (let i = 0; i < companyDataArray.length - 1; i++) {
                const currentOwner = companyDataArray[i];
                const nextOwner = companyDataArray[i + 1];

                if (dealCreatedAt.isBetween(moment(currentOwner.timestamp), moment(nextOwner.timestamp), null, '[)')) { // '[)' start is inclusive/ end is exclusive
                    selectedCompanyData = currentOwner;
                    break;
                }
            }

            if (!selectedCompanyData) {
                selectedCompanyData = companyDataArray[companyDataArray.length - 1];
            }

            filtered.push({
                unique_id: index + 1,
                deal_id: deal.id,
                companyId: deal.companyId,
                createdAt: deal.createdAt,
                hubspotOwnerId: deal.hubspotOwnerId,
                companyData: [selectedCompanyData]
            });
        }

        return filtered;
    }, []);

    console.log(`Following Deals have been updated with new hubspot_owner_id.`);

    // Batch update deals
    const updatePromises = [];
    const batchUpdates = [];

    filteredDeals.forEach((deal, index) => {
        const newOwnerId = deal.companyData[0].value;
        const previousOwnerId = deal.hubspotOwnerId;

        batchUpdates.push({
            deal_id: deal.deal_id,
            newOwnerId: newOwnerId
            // previousOwnerId: previousOwnerId
        });

        if (batchUpdates.length === batchSize || index === filteredDeals.length - 1) {
            updatePromises.push(
                updateDealsBatch(batchUpdates)
                    .then(updatedDeals => {
                        updatedDeals.forEach(updatedDeal => {
                            console.log(`Deal ${updatedDeal.deal_id} --> hubspot_owner_id updated to : ${updatedDeal.newOwnerId}`);
                        });
                    })
                    .catch(error => {
                        console.error('Error updating deals batch:', error);
                    })
            );
            batchUpdates.length = 0;
        }
    });

    await Promise.all(updatePromises);

    if (after) {
        console.log('Fetching next batch of deals...');
        await processDealsBatch(after);
    } else {
        console.log('All deals have been processed.');
    }
}

async function updateDealsBatch(batchUpdates) {
    const updatePromises = batchUpdates.map(async update => {
        const { deal_id, newOwnerId } = update;
        try {
            await updateDealOwner(update.deal_id, update.newOwnerId, apiKey, maxRetries);
            return { deal_id: deal_id, newOwnerId: newOwnerId, success: true };
        } catch (error) {
            console.error(`Error updating deal ${deal_id} owner:`, error);
            return { deal_id: deal_id, newOwnerId: newOwnerId, success: false };
        }
    });

    return Promise.all(updatePromises);
}

processDealsBatch().catch(error => {
    console.error('Error in processDealsBatch:', error);
});
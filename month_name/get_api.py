from googleads import adwords

PATH_TO_YAML = "/home/victor/work/free/marichka_proj/googleads_septa.yaml"


def build_sel(queries_list, criteria):
    selector = {
        'searchParameters': [
            {
                'xsi_type': 'RelatedToQuerySearchParameter',
                'queries': queries_list
            },
            {
                # Network search parameter (optional)
                'xsi_type': 'NetworkSearchParameter',
                'networkSetting': {
                    'targetGoogleSearch': True,
                    'targetSearchNetwork': False,
                    'targetContentNetwork': False,
                    'targetPartnerSearchNetwork': False
                }
            },

            {
                'xsi_type': 'LocationSearchParameter',
                'locations': [{'id': str(criteria)}]
            }
        ],
        'ideaType': 'KEYWORD',
        'requestType': 'STATS',
        'requestedAttributeTypes': ['KEYWORD_TEXT', 'SEARCH_VOLUME','COMPETITION','AVERAGE_CPC',\
                                    'TARGETED_MONTHLY_SEARCHES'],
        'paging': {
            'startIndex': '0',
            'numberResults': '700'
        }
    }
    return selector

client = adwords.AdWordsClient.LoadFromStorage(PATH_TO_YAML)

# подключаем нужный сервис

targeting_idea_service = client.GetService(
      'TargetingIdeaService', version='v201710')
#
# Copyright (c) Dataiku SAS 2019-2023
# 
# This plugin is distributed under the terms of the Apache License version 2.0
#
from dataiku.runnables import Runnable
import dataiku

class GDPRDSCheckUpRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()

    def get_progress_target(self):
        return (100, 'NONE')

    def run(self, progress_callback):
        if self.config.get('allProjects', False):
            project_key_list = [prj['projectKey'] for prj in self.client.list_projects()]
        else:
            project_key_list = [self.project_key]

        callback_progression = (100 / len(project_key_list)) if project_key_list > 0 else 100
        progress = 0;
        progress_callback(progress)
        html_ret = "<html><head><style>" \
            + "h3 { margin-top: 1em; } " \
            + "table { border-collapse: collapse; } " \
            + "thead { font-weight: bold; } " \
            + "table, th, td { border: 1px solid black; }" \
            + "</style></head><body>"

        for project_key in project_key_list:
            # header
            project = self.client.get_project(project_key)
            project_metadata = project.get_metadata()
            html_ret += "<h3>Project " + project_metadata.get("label", project_key) + "</h3>"

            # datasets
            html_ret += "<table><thead>" \
                + "<tr>" \
                + "<th>Dataset</th>" \
                + "<th>Contains personal data</th>" \
                + "<th>Purpose</th>" \
                + "<th>Retention policy</th>" \
                + "<th>Legal consent</th>" \
                + "</tr>" \
                + "</thead><tbody>"
            for dataset_name in [ds["name"] for ds in project.list_datasets()]:
                dataset = project.get_dataset(dataset_name)
                dataset_definition = dataset.get_definition()
                if self.config.get('onlyUnsure', True) and dataset_definition.get("customFields", {}).get("gdpr_contains_personal_data", "UNSURE") != "UNSURE":
                    continue
                html_ret += "<tr>" \
                    + "<td>" + dataset_name + "</td>" \
                    + "<td>" + dataset_definition.get("customFields", {}).get("gdpr_contains_personal_data", "UNSURE") + "</td>" \
                    + "<td>" + dataset_definition.get("customFields", {}).get("gdpr_purposes", "") + "</td>" \
                    + "<td>" + dataset_definition.get("customFields", {}).get("gdpr_retention_policy", "") + "</td>" \
                    + "<td>" + dataset_definition.get("customFields", {}).get("gdpr_legal_consent", "") + "</td>" \
                    + "</tr>"
            html_ret += "</tbody></table>"

            progress += callback_progression
            progress_callback(progress)

        html_ret += "</body></html>"
        return html_ret

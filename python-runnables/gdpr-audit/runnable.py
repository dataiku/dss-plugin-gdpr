#
# Copyright (c) Dataiku SAS 2019-2023
# 
# This plugin is distributed under the terms of the Apache License version 2.0
#
from dataiku.runnables import Runnable
import dataiku
import re
import calendar
from datetime import datetime
import dateutil.parser

class GDPRAuditRunnable(Runnable):
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

        callback_progression = 0
        include_connections = self.config.get('includeConnections', True)
        if include_connections:
            callback_progression += 1
        include_projects = self.config.get('includeProjects', True)
        if include_projects:
            callback_progression += 1
        include_all_objects = self.config.get('includeAllObjects', True)
        if include_all_objects:
            callback_progression += 1

        callback_progression = (100 / callback_progression) if callback_progression > 0 else 100
        progress = 0;
        progress_callback(progress)
        html_ret = "<html><head><style>" \
            + "h3 { margin-top: 1em; } " \
            + "table { border-collapse: collapse; } " \
            + "thead { font-weight: bold; } " \
            + "table, th, td { border: 1px solid black; }" \
            + "</style></head><body>"

        # connections
        if include_connections:
            # header
            html_ret += "<h3>Connections</h3>"
            html_ret += "<table><thead>" \
                + "<tr>" \
                + "<th rowspan=\"2\">Connection</th>" \
                + "<th rowspan=\"2\">Type</th>" \
                + "<th rowspan=\"2\">Host</th>" \
                + "<th colspan=\"2\">DSS Groups</th>" \
                + "<th rowspan=\"2\">Datasets with personal data</th>" \
                + "</tr>" \
                + "<tr>" \
                + "<th>Read</th>" \
                + "<th>Usage</th>" \
                + "</tr>" \
                + "</thead><tbody>"

            connections = self.client.list_connections()

            # prebuild the dataset per connection map
            datasets_per_conn = {}
            for project_key in project_key_list:
                # project
                project = self.client.get_project(project_key)
                project_metadata = project.get_metadata()
                for dataset_name in [ds["name"] for ds in project.list_datasets()]:
                    dataset = project.get_dataset(dataset_name)
                    dataset_definition = dataset.get_definition()
                    dataset_conn = dataset_definition.get("params", {}).get("connection", "")
                    has_pers_data = dataset_definition.get("customFields", {}).get("gdpr_contains_personal_data", "UNSURE")
                    if has_pers_data != "NO":
                        if not dataset_conn in datasets_per_conn:
                            datasets_per_conn[dataset_conn] = []
                        datasets_per_conn[dataset_conn].append(project_key + "." + dataset_name + " (" + has_pers_data + ")")

            # crawl over the connections
            for conn_name, conn in connections.items():
                if "host" in conn.get("params", {}):
                    url = conn.get("params", {}).get("host", "") + ":" + str(conn.get("params", {}).get("port", ""))
                elif "jdbcurl" in conn.get("params", {}):
                    m = re.match(".*:\\/\\/([^\\/]*)\\/.*", conn.get("params", {}).get("jdbcurl", ""))
                    url = m.group(1) if m is not None else ""
                else:
                    url = ""
                html_ret += "<tr>" \
                    + "<td>" + conn_name + "</td>" \
                    + "<td>" + conn.get("type", "") + "</td>" \
                    + "<td>" + url + "</td>" \
                    + "<td>"
                
                readability = conn.get("detailsReadability", {})
                if readability.get("readableBy", "NONE") == "NONE":
                    html_ret += "NONE"
                elif readability.get("readableBy", "NONE") == "ALL":
                    html_ret += "ALL"
                elif readability.get("readableBy", "NONE") == "ALLOWED":
                    first = True
                    html_ret += "<pre>"
                    for grp in readability.get("allowedGroups", []):
                        if not first:
                            html_ret += "\n"
                        html_ret += grp
                        first = False
                    html_ret += "</pre>"
                html_ret += "</td>" \
                    + "<td>"
                if conn.get("usableBy", "ALL") == "ALL":
                    html_ret += "ALL"
                elif conn.get("usableBy", "ALLOWED") == "ALLOWED":
                    first = True
                    html_ret += "<pre>"
                    for grp in conn.get("allowedGroups", []):
                        if not first:
                            html_ret += "\n"
                        html_ret += grp
                        first = False
                    html_ret += "</pre>"
                html_ret += "</td>" \
                    + "<td>"

                if conn_name in datasets_per_conn:
                    html_ret += "<pre>" + "\n".join(datasets_per_conn[conn_name]) + "</pre>"

                html_ret += "</td>" \
                    + "</tr>"

            html_ret += "</tbody></table>"
            progress += callback_progression
            progress_callback(progress)

        if include_projects:
            # header
            html_ret += "<h3>Projects</h3>"
            html_ret += "<table><thead>" \
                + "<tr>" \
                + "<th rowspan=\"2\">Project</th>" \
                + "<th colspan=\"2\">DSS groups</th>" \
                + "<th colspan=\"5\">GDPR fields</th>" \
                + "<th colspan=\"4\">Dataset counts</th>" \
                + "<th colspan=\"2\">Model counts</th>" \
                + "</tr>" \
                + "<tr>" \
                + "<th>Read</th>" \
                + "<th>Write</th>" \
                + "<th>Forbid DS sharing</th>" \
                + "<th>Forbid DS export</th>" \
                + "<th>Forbid model creation</th>" \
                + "<th>Forbid uploaded datasets</th>" \
                + "<th>Forbidden connections</th>" \
                + "<th>Pers. data</th>" \
                + "<th>Unsure</th>" \
                + "<th>No pers. data</th>" \
                + "<th>Total</th>" \
                + "<th>Pers. data or unsure</th>" \
                + "<th>Total</th>" \
                + "</tr>" \
                + "</thead><tbody>"

            for project_key in project_key_list:
                # project
                project = self.client.get_project(project_key)
                project_metadata = project.get_metadata()

                html_ret += "<tr>" \
                    + "<td>" + project_metadata.get("label", project_key) + " (" + project_key + ")" + "</td>"

                # permissions
                permissions = project.get_permissions()
                read_grps = []
                write_grps = []
                for perm in permissions.get("permissions", []):
                    if "group" in perm:
                        if perm.get("writeProjectContent", False):
                            write_grps.append(perm.get("group", ""))
                            read_grps.append(perm.get("group", ""))
                            continue
                        if perm.get("readProjectContent", False):
                            read_grps.append(perm.get("group", ""))

                html_ret += "<td>"
                if len(read_grps) > 0:
                    html_ret += "<pre>" + "\n".join(read_grps) + "</pre>"
                html_ret += "</td>" \
                    + "<td>"
                if len(write_grps) > 0:
                    html_ret += "<pre>" + "\n".join(write_grps) + "</pre>"
                html_ret += "</td>"

                # custom fields
                html_ret += "<td>" + str(project_metadata.get("customFields", {}).get("gdpr_forbid_dataset_sharing", False)) + "</td>" \
                    + "<td>" + str(project_metadata.get("customFields", {}).get("gdpr_forbid_dataset_export", False)) + "</td>" \
                    + "<td>" + str(project_metadata.get("customFields", {}).get("gdpr_forbid_model_creation", False)) + "</td>" \
                    + "<td>" + str(project_metadata.get("customFields", {}).get("gdpr_forbid_uploaded_datasets", False)) + "</td>" \
                    + "<td>"

                allowed_connections = project_metadata.get("customFields", {}).get("gdpr_forbidden_connections", [])
                if len(allowed_connections) > 0:
                    html_ret += "<pre>" + "\n".join(allowed_connections) + "</pre>"
                html_ret += "</td>"

                # dataset counts
                ds_yes = 0
                ds_unsure = 0
                ds_no = 0
                ds_total = 0
                for dataset_name in [ds["name"] for ds in project.list_datasets()]:
                    ds_total += 1
                    dataset = project.get_dataset(dataset_name)
                    dataset_definition = dataset.get_definition()
                    has_pers_data = dataset_definition.get("customFields", {}).get("gdpr_contains_personal_data", "UNSURE")
                    if has_pers_data == "YES":
                        ds_yes += 1
                    elif has_pers_data == "UNSURE":
                        ds_unsure += 1
                    elif has_pers_data == "NO":
                        ds_no += 1
                html_ret += "<td>" + str(ds_yes) + "</td>" \
                    + "<td>" + str(ds_unsure) + "</td>" \
                    + "<td>" + str(ds_no) + "</td>" \
                    + "<td>" + str(ds_total) + "</td>"

                # model counts
                ml_tasks = project.list_ml_tasks().get("mlTasks", [])
                count_mlt_total = 0
                count_mlt_pers_data = 0
                for ml_task in ml_tasks:
                    count_mlt_total += 1
                    ml_ds_smartname = ml_task.get("inputDataset", "")
                    if len(ml_ds_smartname) > 0:
                        ml_pkey = project_key if not "." in ml_ds_smartname else ml_ds_smartname.split(".")[0]
                        ml_input_ds = ml_ds_smartname if not "." in ml_ds_smartname else ml_ds_smartname.split(".")[1]
                        ml_dataset = self.client.get_project(ml_pkey).get_dataset(ml_input_ds)
                        ml_dataset_definition = ml_dataset.get_definition()
                        ml_ds_has_pers_data = ml_dataset_definition.get("customFields", {}).get("gdpr_contains_personal_data", "UNSURE")
                        if ml_ds_has_pers_data != "NO":
                            count_mlt_pers_data += 1
                html_ret += "<td>" + str(count_mlt_pers_data) + "</td>" \
                    + "<td>" + str(count_mlt_total) + "</td>" \
                    + "</tr>"

            html_ret += "</tbody></table>"
            progress += callback_progression
            progress_callback(progress)

        if include_all_objects:
            # header
            html_ret += "<h3>All objects</h3>"

            for project_key in project_key_list:
                # project
                project = self.client.get_project(project_key)
                project_metadata = project.get_metadata()
                project_settings = project.get_settings().settings
                project_exposed_objects = project_settings.get("exposedObjects", {}).get("objects", [])

                # project header
                html_ret += "<h4>Project " + project_metadata.get("label", project_key) + " (" + project_key + ")" + "</h4>"

                # dataset
                html_ret += "<h5>Datasets</h5>" \
                    + "<table><thead>" \
                    + "<tr>" \
                    + "<th rowspan=\"2\">Dataset</td>" \
                    + "<th rowspan=\"2\">Is source?</td>" \
                    + "<th colspan=\"4\">Columns</th>" \
                    + "<th rowspan=\"2\">Creation date</th>" \
                    + "<th rowspan=\"2\">Last build date</th>" \
                    + "<th rowspan=\"2\">Projects shared with</th>" \
                    + "<th rowspan=\"2\">Description</th>" \
                    + "<th colspan=\"4\">GDPR fields</th>" \
                    + "</tr>" \
                    + "<tr>" \
                    + "<th>Name</th>" \
                    + "<th>Type</th>" \
                    + "<th>Comment</th>" \
                    + "<th>Meaning</th>" \
                    + "<th>Contains pers. data</th>" \
                    + "<th>Purposes</th>" \
                    + "<th>Retention policy</th>" \
                    + "<th>Legal consent</th>" \
                    + "</tr>" \
                    + "</thead><tbody>"

                for dataset_name in [ds["name"] for ds in project.list_datasets()]:
                    dataset = project.get_dataset(dataset_name)
                    dataset_definition = dataset.get_definition()
                    dataset_metadata = dataset.get_metadata()
                    dataset_metrics = dataset.get_last_metric_values()
                    dataset_columns = dataset_definition.get("schema", {}).get("columns", [])
                    dataset_usages = dataset.get_usages()
                    dataset_is_source = len([item for item in dataset_usages if item.get("type", "") == "RECIPE_OUTPUT"]) == 0

                    # get the number of columns for the rowspan layout
                    dataset_col_nb = len(dataset_columns)
                    dataset_rowspan_html = " rowspan=\"" + str(dataset_col_nb) + "\"" if dataset_col_nb > 1 else ""

                    html_ret += "<tr>" \
                        + "<td" + dataset_rowspan_html + ">" + dataset_name + "</td>" \
                        + "<td" + dataset_rowspan_html + ">" + ("YES" if dataset_is_source else "") + "</td>"

                    if dataset_col_nb > 0:
                        column = dataset_columns[0]
                        html_ret += "<td>" + column.get("name", "") + "</td>" \
                            + "<td>" + column.get("type", "") + "</td>" \
                            + "<td>" + column.get("comment", "") + "</td>" \
                            + "<td>" + column.get("meaning", "") + "</td>"
                    else:
                        html_ret += "<td></td>" \
                            + "<td></td>" \
                            + "<td></td>" \
                            + "<td></td>"

                    ds_creation_date = dataset_definition.get("creationTag", {}).get("lastModifiedOn", 0) / 1000
                    date_str = datetime.utcfromtimestamp(ds_creation_date).strftime('%Y-%m-%d %H:%M:%S') if ds_creation_date > 0 else ""
                    html_ret += "<td" + dataset_rowspan_html + ">" + date_str + "</td>" \
                        + "<td" + dataset_rowspan_html + ">"
                    if "reporting:BUILD_START_DATE" in dataset_metrics.get_all_ids():
                        max_timestamp = 0
                        for val in dataset_metrics.get_metric_by_id("reporting:BUILD_START_DATE").get("lastValues", []):
                            timestamp = calendar.timegm(dateutil.parser.parse(val.get("value", "")).timetuple())
                            if timestamp > max_timestamp:
                                max_timestamp = timestamp
                        if max_timestamp > 0:
                            html_ret += datetime.utcfromtimestamp(max_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                    html_ret += "</td>"

                    project_shared = []
                    for obj in project_exposed_objects:
                        if obj.get("type", "") == "DATASET" and obj.get("localName", "") == dataset_name:
                            for rule in obj.get("rules", []):
                                target_prj = rule.get("targetProject", "")
                                if target_prj != "":
                                    project_shared.append(target_prj)
                    html_ret += "<td" + dataset_rowspan_html + ">"
                    if len(project_shared) > 0:
                        html_ret += "<pre>" + "\n".join(project_shared) + "</pre>"
                    html_ret += "</td>" \
                        + "<td" + dataset_rowspan_html + ">" + dataset_metadata.get("description", "") + "</td>" \
                        + "<td" + dataset_rowspan_html + ">" + dataset_definition.get("customFields", {}).get("gdpr_contains_personal_data", "UNSURE") + "</td>" \
                        + "<td" + dataset_rowspan_html + ">" + dataset_definition.get("customFields", {}).get("gdpr_purposes", "") + "</td>" \
                        + "<td" + dataset_rowspan_html + ">" + dataset_definition.get("customFields", {}).get("gdpr_retention_policy", "") + "</td>" \
                        + "<td" + dataset_rowspan_html + ">" + dataset_definition.get("customFields", {}).get("gdpr_legal_consent", "") + "</td>" \
                        + "</tr>"

                    if dataset_col_nb > 1:
                        for i in range(1, dataset_col_nb):
                            column = dataset_columns[i]
                            html_ret += "<tr>" \
                                + "<td>" + column.get("name", "") + "</td>" \
                                + "<td>" + column.get("type", "") + "</td>" \
                                + "<td>" + column.get("comment", "") + "</td>" \
                                + "<td>" + column.get("meaning", "") + "</td>" \
                                + "</tr>"

                html_ret += "</tbody></table>"

                # analyses
                html_ret += "<h5>Analysis</h5>" \
                    + "<table><thead>" \
                    + "<tr>" \
                    + "<th rowspan=\"2\">ID</th>" \
                    + "<th rowspan=\"2\">Name</th>" \
                    + "<th rowspan=\"2\">Creation date</th>" \
                    + "<th rowspan=\"2\">Dataset</th>" \
                    + "<th colspan=\"3\">Models</th>" \
                    + "</tr>" \
                    + "<tr>" \
                    + "<th>Type</th>" \
                    + "<th>Features</th>" \
                    + "<th>Last train date</th>" \
                    + "</tr>" \
                    + "</thead><tobdy>"

                for analysis_info in project.list_analyses():
                    analysis_id = analysis_info.get("analysisId", "")
                    analysis = project.get_analysis(analysis_id)
                    analysis_definition = analysis.get_definition().get_raw()
                    analysis_ml_tasks = analysis.list_ml_tasks().get("mlTasks", [])

                    # get the number of ml tasks
                    analysis_ml_tasks_nb = len(analysis_ml_tasks)
                    analysis_rowspan_html = " rowspan=\"" + str(analysis_ml_tasks_nb) + "\"" if analysis_ml_tasks_nb > 1 else ""

                    html_ret += "<tr>" \
                        + "<td" + analysis_rowspan_html + ">" + analysis_definition.get("id", "") + "</td>" \
                        + "<td" + analysis_rowspan_html + ">" + analysis_definition.get("name", "") + "</td>"

                    analysis_creation_date = analysis_definition.get("creationTag", {}).get("lastModifiedOn", 0) / 1000
                    date_str = datetime.utcfromtimestamp(analysis_creation_date).strftime('%Y-%m-%d %H:%M:%S') if analysis_creation_date > 0 else ""
                    html_ret += "<td" + analysis_rowspan_html + ">" + date_str + "</td>" \
                        + "<td" + analysis_rowspan_html + ">" + analysis_definition.get("inputDatasetSmartName", "") + "</td>"

                    if analysis_ml_tasks_nb > 0:
                        analysis_ml_task = analysis_ml_tasks[0]
                        ml_task = project.get_ml_task(analysis_id, analysis_ml_task.get("mlTaskId", ""))
                        ml_task_settings = ml_task.get_settings().get_raw()

                        ml_task_type = ml_task_settings.get("taskType", "")
                        ml_task_sub_type = (": " + ml_task_settings.get("predictionType", "")) if ml_task_type == "PREDICTION" else ""
                        html_ret += "<td>" + ml_task_type + ml_task_sub_type + "</td>" \
                            + "<td>"

                        features = []
                        for feat_name, feat_info in ml_task_settings.get("preprocessing", {}).get("per_feature", {}).items():
                            feat_role = feat_info.get("role", "REJECT")
                            if feat_role == "REJECT":
                                continue
                            features.append(feat_name + ((" (" + feat_role + ")") if feat_role != "INPUT" else ""))
                        if len(features) > 0:
                            html_ret += "<pre>" + "\n".join(features) + "</pre>"
                        html_ret += "</td>" \
                            + "<td>"

                        max_train_date = 0
                        for trained_model_id in ml_task.get_trained_models_ids():
                            trained_model = ml_task.get_trained_model_details(trained_model_id)
                            train_date = trained_model.get_train_info().get("startTime", 0) / 1000
                            if train_date > max_train_date:
                                max_train_date = train_date
                        if max_train_date > 0:
                            html_ret += datetime.utcfromtimestamp(max_train_date).strftime('%Y-%m-%d %H:%M:%S')
                        html_ret += "</td>"
                    else:
                        html_ret += "<td></td>" \
                            + "<td></td>" \
                            + "<td></td>"

                    html_ret += "</tr>"

                    if analysis_ml_tasks_nb > 1:
                        for i in range(1, analysis_ml_tasks_nb):
                            analysis_ml_task = analysis_ml_tasks[i]
                            ml_task = project.get_ml_task(analysis_id, analysis_ml_task.get("mlTaskId", ""))
                            ml_task_settings = ml_task.get_settings().get_raw()

                            ml_task_type = ml_task_settings.get("taskType", "")
                            ml_task_sub_type = (": " + ml_task_settings.get("predictionType", "")) if ml_task_type == "PREDICTION" else ""
                            html_ret += "<td>" + ml_task_type + ml_task_sub_type + "</td>" \
                                + "<td>"

                            features = []
                            for feat_name, feat_info in ml_task_settings.get("preprocessing", {}).get("per_feature", {}).items():
                                feat_role = feat_info.get("role", "REJECT")
                                if feat_role == "REJECT":
                                    continue
                                features.append(feat_name + ((" (" + feat_role + ")") if feat_role != "INPUT" else ""))
                            if len(features) > 0:
                                html_ret += "<pre>" + "\n".join(features) + "</pre>"
                            html_ret += "</td>" \
                                + "<td>"

                            max_train_date = 0
                            for trained_model_id in ml_task.get_trained_models_ids():
                                trained_model = ml_task.get_trained_model_details(trained_model_id)
                                train_date = trained_model.get_train_info().get("startTime", 0) / 1000
                                if train_date > max_train_date:
                                    max_train_date = train_date
                            if max_train_date > 0:
                                html_ret += datetime.utcfromtimestamp(max_train_date).strftime('%Y-%m-%d %H:%M:%S')
                            html_ret += "</td>" \
                                + "</tr>"

                html_ret += "</tbody></table>"

                # saved models
                html_ret += "<h5>Saved models</h5>" \
                    + "<table><thead>" \
                    + "<tr>" \
                    + "<th>Name</th>" \
                    + "<th>Type</th>" \
                    + "<th>Features</th>" \
                    + "<th>Original analysis ID</th>" \
                    + "<th>Train date</th>" \
                    + "</tr>" \
                    + "</thead><tobdy>"

                for saved_model_info in project.list_saved_models():
                    saved_model_id = saved_model_info.get("id", "")
                    saved_model_name = saved_model_info.get("name", "")
                    saved_model_type = saved_model_info.get("type", "")
                    saved_model = project.get_saved_model(saved_model_id)

                    saved_model_version_info = saved_model.get_active_version()
                    saved_model_version_train_date = saved_model_version_info.get("trainDate", 0) / 1000
                    saved_model_version_id = saved_model_version_info.get("id", "")
                    saved_model_version = saved_model.get_version_details(saved_model_version_id).details
                    saved_model_sub_type = (": " + saved_model_version.get("coreParams", {}).get("prediction_type", "")) if saved_model_type == "PREDICTION" else ""

                    html_ret += "<tr>" \
                        + "<td>" + saved_model_name + "</td>" \
                        + "<td>" + saved_model_type + saved_model_sub_type + "</td>" \
                        + "<td>"

                    features = []
                    for feat_name, feat_info in saved_model_version.get("preprocessing", {}).get("per_feature", {}).items():
                        feat_role = feat_info.get("role", "REJECT")
                        if feat_role == "REJECT":
                            continue
                        features.append(feat_name + ((" (" + feat_role + ")") if feat_role != "INPUT" else ""))
                    if len(features) > 0:
                        html_ret += "<pre>" + "\n".join(features) + "</pre>"
                    html_ret += "</td>" \
                        + "<td>"

                    full_model_id = saved_model_version.get("smOrigin", {}).get("fullModelId", "")
                    if full_model_id != "":
                        full_model_id_parts = full_model_id.split("-")
                        if len(full_model_id_parts) > 2:
                            html_ret += full_model_id_parts[2]

                    html_ret += "</td>" \
                        + "<td>"
                    if saved_model_version_train_date > 0:
                        html_ret += datetime.utcfromtimestamp(saved_model_version_train_date).strftime('%Y-%m-%d %H:%M:%S')
                    html_ret += "</td>" \
                        + "</tr>"

                html_ret += "</tbody></table>"

        html_ret += "</body></html>"
        return html_ret

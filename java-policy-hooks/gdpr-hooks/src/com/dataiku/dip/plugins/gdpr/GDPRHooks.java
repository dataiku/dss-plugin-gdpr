/**
 * Copyright (c) Dataiku SAS 2019-2023
 * 
 * This plugin is distributed under the terms of the Apache License version 2.0
 */
package com.dataiku.dip.plugins.gdpr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dataiku.dip.exceptions.CodedException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;


import com.dataiku.dip.analysis.model.MLTask;
import com.dataiku.dip.analysis.model.core.AnalysisCoreParams;
import com.dataiku.dip.coremodel.Dataset;
import com.dataiku.dip.coremodel.ExposedObject;
import com.dataiku.dip.coremodel.InfoMessage.FixabilityCategory;
import com.dataiku.dip.coremodel.InfoMessage.MessageCode;
import com.dataiku.dip.coremodel.SerializedDataset;
import com.dataiku.dip.coremodel.SerializedProject;
import com.dataiku.dip.coremodel.SerializedRecipe;
import com.dataiku.dip.coremodel.SerializedRecipe.RecipeInput;
import com.dataiku.dip.coremodel.SerializedRecipe.RecipeOutput;
import com.dataiku.dip.cuspol.CustomPolicyHooks;
import com.dataiku.dip.datasets.fs.BuiltinFSDatasets.UploadedFilesConfig;
import com.dataiku.dip.export.ExportParams;
import com.dataiku.dip.export.ExportStatus.ExportMethod;
import com.dataiku.dip.export.input.ExportDataset;
import com.dataiku.dip.export.input.ExportInput;
import com.dataiku.dip.export.input.ExportShaker;
import com.dataiku.dip.futures.FuturePayload.FuturePayloadTarget;
import com.dataiku.dip.plugins.RegularPluginsRegistryService;
import com.dataiku.dip.projects.importexport.model.ProjectExportOptions;
import com.dataiku.dip.security.AuthCtx;
import com.dataiku.dip.server.datasets.DatasetAccessService;
import com.dataiku.dip.server.datasets.DatasetSaveService;
import com.dataiku.dip.server.datasets.DatasetSaveService.DatasetCreationContext;
import com.dataiku.dip.server.datasets.DatasetSaveService.DatasetCreationContext.DatasetCreationType;
import com.dataiku.dip.server.services.ProjectsService;
import com.dataiku.dip.server.services.ITaggingService.TaggableType;
import com.dataiku.dip.server.services.TaggableObjectsService.TaggableObject;
import com.dataiku.dip.util.AnyLoc;
import com.dataiku.dip.utils.DKULogger;
import com.dataiku.dip.utils.JSON;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class GDPRHooks extends CustomPolicyHooks {
    @Autowired private ProjectsService projectsService;
    @Autowired private DatasetAccessService datasetAccessService;
    @Autowired private DatasetSaveService datasetSaveService;
    @Autowired private RegularPluginsRegistryService regularPluginsRegistryService;

    @Override
    public void onPreDatasetCreation(AuthCtx user, SerializedDataset serializedDataset, DatasetCreationContext context) throws Exception {
        MessageCode mc = new MessageCode() {
            @Override
            public String getCode() {
                return "ERR_GDPR_DATASET_CREATION";
            }

            @Override
            public String getCodeTitle() {
                return "Cannot create dataset";
            }

            @Override
            public FixabilityCategory getFixability() {
                return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
            }
        };

        // check sharing settings on origin project if copied from it
        boolean hasPersonalData = !serializedDataset.customFields.has("gdpr_contains_personal_data") || !"NO".equals(serializedDataset.customFields.get("gdpr_contains_personal_data").getAsString());
        if (hasPersonalData && context.type == DatasetCreationType.COPY_FROM_ANOTHER_PROJECT) {
            SerializedProject originPrj = projectsService.getMandatoryUnsafe(context.originProjectKey);
            boolean forbidSharing = originPrj.customFields != null && originPrj.customFields.has("gdpr_forbid_dataset_sharing") && originPrj.customFields.get("gdpr_forbid_dataset_sharing").getAsBoolean();
            if (forbidSharing) {
                throw new CodedException(mc, "GDPR policies on this project forbid sharing datasets that contain personal data (dataset: " + context.originDatasetName + ")");
            }
        }

        // check connection with settings in actual project
        SerializedProject sp = projectsService.getMandatoryUnsafe(serializedDataset.projectKey);
        Set<String> forbiddenConnections = new HashSet<>();
        if (sp.customFields.has("gdpr_forbidden_connections")) {
            for (JsonElement conn : sp.customFields.get("gdpr_forbidden_connections").getAsJsonArray()) {
                if (StringUtils.isNotBlank(conn.getAsString())) {
                    forbiddenConnections.add(conn.getAsString());
                }
            }
        }
        if (serializedDataset.getParams().getConnection() != null) {
            if (forbiddenConnections.contains(serializedDataset.getParams().getConnection())) {
                throw new CodedException(mc, "GDPR policies on this project forbid creating datasets on connection " + serializedDataset.getParams().getConnection() + " (dataset: " + serializedDataset.name + ")");
            }
        } else if ("UploadedFiles".equals(serializedDataset.getSubtype())) {
            if (sp.customFields.has("gdpr_forbid_uploaded_datasets") && sp.customFields.get("gdpr_forbid_uploaded_datasets").getAsBoolean()) {
                throw new CodedException(mc, "GDPR policies on this project forbid creating uploaded datasets (dataset: " + serializedDataset.name + ")");
            }
            if (serializedDataset.getParams() instanceof UploadedFilesConfig) {
                UploadedFilesConfig upCfg = (UploadedFilesConfig) serializedDataset.getParams();
                if (forbiddenConnections.contains(upCfg.uploadConnection)) {
                    throw new CodedException(mc, "GDPR policies on this project forbid creating datasets on connection " + upCfg.uploadConnection + " (dataset: " + serializedDataset.name + ")");
                }
            }
        }
    }

    @Override
    public void onPreMLModelCreation(AuthCtx user, SerializedDataset sd, AnalysisCoreParams analysis, MLTask mlTask)  throws Exception {
        SerializedProject sp = projectsService.getMandatoryUnsafe(analysis.projectKey);
        MessageCode mc = new MessageCode() {
            @Override
            public String getCode() {
                return "ERR_GDPR_MODEL_CREATION";
            }

            @Override
            public String getCodeTitle() {
                return "Cannot create model";
            }

            @Override
            public FixabilityCategory getFixability() {
                return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
            }
        };
        boolean hasPersonalData = !sd.customFields.has("gdpr_contains_personal_data") || !"NO".equals(sd.customFields.get("gdpr_contains_personal_data").getAsString());
        boolean forbidCreation = sp.customFields.has("gdpr_forbid_model_creation") && sp.customFields.get("gdpr_forbid_model_creation").getAsBoolean();
        if (hasPersonalData && forbidCreation) {
            throw new CodedException(mc, "GDPR policies on this project forbid model creation on datasets that contain personal data (dataset: " + sd.name + ")");
        }
    }

    @Override
    public void onPreSharedItemsSave(AuthCtx user, SerializedProject before, SerializedProject after) throws Exception {
        boolean forbidSharing = before != null && before.customFields != null &&
            before.customFields.has("gdpr_forbid_dataset_sharing") && before.customFields.get("gdpr_forbid_dataset_sharing").getAsBoolean();
        if (!forbidSharing) {
            return;
        }

        boolean beforeNull = before == null || before.exposedObjects == null;
        boolean bothNulls = beforeNull && after.exposedObjects == null;
        boolean sameValue = bothNulls || (!beforeNull && before.exposedObjects.equals(after.exposedObjects));
        if (sameValue || after.exposedObjects == null || after.exposedObjects.objects == null || after.exposedObjects.objects.isEmpty()) {
            return;
        }

        MessageCode mc = new MessageCode() {
            @Override
            public String getCode() {
                return "ERR_GDPR_SHARED_ITEM_SAVE";
            }

            @Override
            public String getCodeTitle() {
                return "Cannot save shared items";
            }

            @Override
            public FixabilityCategory getFixability() {
                return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
            }
        };
        for (ExposedObject eo : after.exposedObjects.objects) {
            if (eo.type != TaggableType.DATASET) {
                continue;
            }
            boolean eoHasChanged = true;
            if (!beforeNull && before.exposedObjects.objects != null && !before.exposedObjects.objects.isEmpty()) {
                boolean beoFound = false;
                for (ExposedObject beo : before.exposedObjects.objects) {
                    if (eo.localName.equals(beo.localName)) {
                        if (!beoFound) { // first time the eo is found, assign the value directly (override)
                            eoHasChanged = !beo.equals(eo);
                        } else { // any other times after that, eo is considered changed if is has changed once
                            eoHasChanged = eoHasChanged || !beo.equals(eo);
                        }
                        beoFound = true;
                        // we found eo at least once and it has changed, so we can end the loop
                        if (eoHasChanged) {
                            break;
                        }
                    }
                }
            }
            if (eoHasChanged) {
                SerializedDataset sd = datasetAccessService.getMandatoryUnsafe(AnyLoc.resolveSmart(before.projectKey, eo.localName)).getModel();
                boolean hasPersonalData = !sd.customFields.has("gdpr_contains_personal_data") || !"NO".equals(sd.customFields.get("gdpr_contains_personal_data").getAsString());
                if (hasPersonalData) {
                    throw new CodedException(mc, "GDPR policies on this project forbid sharing datasets that contain personal data (dataset: " + sd.name + ")");
                }
            }
        }
    }

    @Override
    public void onPreDataExport(AuthCtx user, ExportInput input, ExportParams params, ExportMethod exportMethod) throws Exception {
        // took into account only dataset and shaker exports (TODO need to investigate the other cases here)
        if (!(input instanceof ExportDataset) && !(input instanceof ExportShaker)) {
            return;
        }
        FuturePayloadTarget fpt = input.getSource();
        SerializedProject sp = projectsService.getMandatoryUnsafe(fpt.projectKey);
        SerializedDataset sd = datasetAccessService.getMandatoryUnsafe(AnyLoc.resolveSmart(fpt.projectKey, fpt.objectId)).getModel();
        MessageCode mc = new MessageCode() {
            @Override
            public String getCode() {
                return "ERR_GDPR_EXPORT_DATASET";
            }

            @Override
            public String getCodeTitle() {
                return "Cannot export dataset";
            }

            @Override
            public FixabilityCategory getFixability() {
                return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
            }
        };
        boolean hasPersonalData = !sd.customFields.has("gdpr_contains_personal_data") || !"NO".equals(sd.customFields.get("gdpr_contains_personal_data").getAsString());
        boolean forbidExport = sp.customFields.has("gdpr_forbid_dataset_export") && sp.customFields.get("gdpr_forbid_dataset_export").getAsBoolean();
        if (hasPersonalData && forbidExport) {
            throw new CodedException(mc, "GDPR policies on this project forbid exporting datasets that contain personal data (dataset: " + sd.name + ")");
        }
    }

    @Override
    public void onPreProjectExport(AuthCtx user, String projectKey, ProjectExportOptions exportOptions) throws Exception {
        SerializedProject sp = projectsService.getMandatoryUnsafe(projectKey);
        MessageCode mc = new MessageCode() {
            @Override
            public String getCode() {
                return "ERR_GDPR_EXPORT_PROJECT";
            }

            @Override
            public String getCodeTitle() {
                return "Cannot export project";
            }

            @Override
            public FixabilityCategory getFixability() {
                return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
            }
        };
        boolean exportHasData = exportOptions.exportUploads || exportOptions.exportManagedFS || exportOptions.exportAnalysisModels ||
            exportOptions.exportSavedModels || exportOptions.exportManagedFolders || exportOptions.exportAllInputDatasets ||
            exportOptions.exportAllDatasets || exportOptions.exportAllInputManagedFolders || exportOptions.exportInsightsData ||
            (exportOptions.includedDatasetsData != null && !exportOptions.includedDatasetsData.isEmpty()) ||
            (exportOptions.includedManagedFolders != null && !exportOptions.includedManagedFolders.isEmpty()) ||
            (exportOptions.includedSavedModels != null && !exportOptions.includedSavedModels.isEmpty());
        boolean forbidExport = sp.customFields.has("gdpr_forbid_dataset_export") && sp.customFields.get("gdpr_forbid_dataset_export").getAsBoolean();
        if (exportHasData && forbidExport) {
            throw new CodedException(mc, "GDPR policies on this project forbid exporting the project if it includes data");
        }
    }

    @Override
    public void onPreObjectSave(AuthCtx user, TaggableObject before, TaggableObject after) throws Exception {
        JsonObject pluginSettings = regularPluginsRegistryService.getSettings("gdpr").config;
        if (after instanceof SerializedProject) {
            MessageCode mc = new MessageCode() {
                @Override
                public String getCode() {
                    return "ERR_GDPR_PROJECT_SAVE";
                }

                @Override
                public String getCodeTitle() {
                    return "Cannot save project";
                }

                @Override
                public FixabilityCategory getFixability() {
                    return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
                }
            };

            // handle admin cf on project
            List<CustomFieldNameWithDefaultValue> adminCfs = new ArrayList<>();
            adminCfs.add(new CustomFieldNameWithDefaultValue("gdpr_forbid_dataset_sharing", new JsonPrimitive(false)));
            adminCfs.add(new CustomFieldNameWithDefaultValue("gdpr_forbid_dataset_export", new JsonPrimitive(false)));
            adminCfs.add(new CustomFieldNameWithDefaultValue("gdpr_forbid_model_creation", new JsonPrimitive(false)));
            adminCfs.add(new CustomFieldNameWithDefaultValue("gdpr_forbid_uploaded_datasets", new JsonPrimitive(false)));
            adminCfs.add(new CustomFieldNameWithDefaultValue("gdpr_forbidden_connections", null));
            CustomFieldsChangeRule adminRule = new CustomFieldsChangeRule(pluginSettings, "gdpr_admin_groups", adminCfs);
            handleCustomFieldsChange(user, before == null ? null : before.customFields, after == null ? null : after.customFields, adminRule, mc);

        } else if (after instanceof SerializedDataset) {
            MessageCode mc = new MessageCode() {
                @Override
                public String getCode() {
                    return "ERR_GDPR_DATASET_SAVE";
                }

                @Override
                public String getCodeTitle() {
                    return "Cannot save dataset";
                }

                @Override
                public FixabilityCategory getFixability() {
                    return FixabilityCategory.ADMIN_SETTINGS_PLUGINS;
                }
            };

            // handle doc custom fields for dataset
            List<CustomFieldNameWithDefaultValue> docCfs = new ArrayList<>();
            docCfs.add(new CustomFieldNameWithDefaultValue("gdpr_contains_personal_data", new JsonPrimitive("UNSURE")));
            docCfs.add(new CustomFieldNameWithDefaultValue("gdpr_purposes", null));
            docCfs.add(new CustomFieldNameWithDefaultValue("gdpr_retention_policy", null));
            docCfs.add(new CustomFieldNameWithDefaultValue("gdpr_legal_consent", null));
            CustomFieldsChangeRule docRule = new CustomFieldsChangeRule(pluginSettings, "gdpr_doc_groups", docCfs);
            handleCustomFieldsChange(user, before == null ? null : before.customFields, after == null ? null : after.customFields, docRule, mc);

            // check if connection has changed
            String afterConnection = ((SerializedDataset) after).getParams().getConnection();
            boolean connHasChanged = before == null || (afterConnection != null && !StringUtils.equals(((SerializedDataset) before).getParams().getConnection(), afterConnection));
            if (connHasChanged) {
                SerializedProject sp = projectsService.getMandatoryUnsafe(((SerializedDataset) after).projectKey);
                Set<String> forbiddenConnections = new HashSet<>();
                if (sp.customFields.has("gdpr_forbidden_connections")) {
                    for (JsonElement conn : sp.customFields.get("gdpr_forbidden_connections").getAsJsonArray()) {
                        if (StringUtils.isNotBlank(conn.getAsString())) {
                            forbiddenConnections.add(conn.getAsString());
                        }
                    }
                }
                if (forbiddenConnections.contains(afterConnection)) {
                    throw new CodedException(mc, "GDPR policies on this project forbid using connection " + afterConnection + " (dataset: " + (before != null ? ((SerializedDataset) before).name : ((SerializedDataset) after).name) + ")");
                }
            }

        } else if (after instanceof SerializedRecipe) {
            // handle recipe save cf propagation
            handleRecipeInputsOutputs(user, (SerializedRecipe) before, (SerializedRecipe) after);

        }
    }

    private static class CustomFieldNameWithDefaultValue {
        private final String name;
        private final JsonElement defaultValue;
        private CustomFieldNameWithDefaultValue(String name, JsonElement defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }
    }

    private static class CustomFieldsChangeRule {
        private final JsonObject pluginSettings;
        private final String key;
        private final List<CustomFieldNameWithDefaultValue> customFieldNames;
        private CustomFieldsChangeRule(JsonObject pluginSettings, String key, List<CustomFieldNameWithDefaultValue> customFieldNames) {
            this.pluginSettings = pluginSettings;
            this.key = key;
            this.customFieldNames = customFieldNames;
        }
    }

    private void handleCustomFieldsChange(AuthCtx user, JsonObject before, JsonObject after, CustomFieldsChangeRule rule, MessageCode mc) throws Exception {
        // check if the user is in group, so allowed to modify the specific CFs
        boolean hasGroupsKey = rule.pluginSettings != null && rule.pluginSettings.has(rule.key);
        String groups = hasGroupsKey ? rule.pluginSettings.get(rule.key).getAsString() : null;
        if (user.isGroupsAware() && groups != null) {
            for (String group : groups.split(",")) {
                if (user.isInGroup(group)) {
                    return;
                }
            }
        }
        // from here we need to forbid every custom fields that has changed (user not in group)
        for (CustomFieldNameWithDefaultValue cfWDv : rule.customFieldNames) {
            String cfName = cfWDv.name;
            boolean beforeMissingCf = before == null || !before.has(cfName);
            boolean afterMissingCf = after == null || !after.has(cfName);
            JsonElement beforeValue = beforeMissingCf ? cfWDv.defaultValue : before.get(cfName);
            JsonElement afterValue = afterMissingCf ? cfWDv.defaultValue : after.get(cfName);
            boolean bothNulls = beforeValue == null && afterValue == null;
            boolean beforeOnlyNull = beforeValue == null && afterValue != null;
            boolean afterOnlyNull = beforeValue != null && afterValue == null;
            if (!bothNulls && (beforeOnlyNull || afterOnlyNull || !JSON.jsonEquals(beforeValue, afterValue))) {
                throw new CodedException(mc, "GDPR policies forbid you to edit metadata field " + cfName + " since you do not belong to one of the following groups: " + (groups == null ? "" : groups));
            }
        }
    }

    private void handleRecipeInputsOutputs(AuthCtx user, SerializedRecipe recipeBeforeChanges, SerializedRecipe recipeToSave) throws Exception {
        // iterate over the inputs to check if they contain perso data
        boolean hasPersoDataOrUnsureInput = false;
        for (RecipeInput input : recipeToSave.getFlatInputs()) {
            Dataset ds = datasetAccessService.getOrNullUnsafe(input.getLoc(recipeToSave.projectKey));
            // if dataset is not found, it's probably a ml recipe, skip it
            if (ds == null) {
                continue;
            }
            SerializedDataset sd = ds.getModel();
            if (!sd.customFields.has("gdpr_contains_personal_data") || !"NO".equals(sd.customFields.get("gdpr_contains_personal_data").getAsString())) {
                hasPersoDataOrUnsureInput = true;
                break;
            }
        }
        // if at least one of the input has possibly pers data, change the value of the cf from "NO" to "UNSURE"
        if (hasPersoDataOrUnsureInput) {
            for (RecipeOutput output : recipeToSave.getFlatOutputs()) {
                Dataset ds = datasetAccessService.getOrNullUnsafe(output.getLoc(recipeToSave.projectKey));
                // if dataset is not found, it's probably a ml recipe, skip it
                if (ds == null) {
                    continue;
                }
                SerializedDataset sd = ds.getModel();
                if (sd.customFields.has("gdpr_contains_personal_data") && "NO".equals(sd.customFields.get("gdpr_contains_personal_data").getAsString())) {
                    sd.customFields.addProperty("gdpr_contains_personal_data", "UNSURE");
                    datasetSaveService.save(ds.getLoc(), sd, user);
                }
            }
        }
    }

    private static DKULogger logger = DKULogger.getLogger("dku.plugins.gdpr.hooks");
}

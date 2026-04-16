import { ZH_CN_MESSAGES } from "./locales/zh-CN.js";
import { EN_US_MESSAGES } from "./locales/en-US.js";

const ADMIN_LOCALE_STORAGE_KEY = "jmqx_admin_locale";

const LOCALE_DEFINITIONS = Object.freeze([
    {
        value: "zh-CN",
        labelKey: "locale.zh-CN",
        messages: ZH_CN_MESSAGES
    },
    {
        value: "en-US",
        labelKey: "locale.en-US",
        messages: EN_US_MESSAGES
    }
]);

const DEFAULT_ADMIN_LOCALE = "zh-CN";

const LOCALE_MESSAGES = LOCALE_DEFINITIONS.reduce((result, localeDefinition) => {
    result[localeDefinition.value] = localeDefinition.messages;
    return result;
}, {});

export function getAdminLocaleOptions() {
    return LOCALE_DEFINITIONS.map((localeDefinition) => ({
        value: localeDefinition.value,
        labelKey: localeDefinition.labelKey
    }));
}

function formatMessage(template, params) {
    if (!params) {
        return template;
    }
    return String(template).replace(/\{(\w+)\}/g, (match, key) => {
        if (Object.prototype.hasOwnProperty.call(params, key)) {
            return String(params[key]);
        }
        return match;
    });
}

function isSupportedAdminLocale(locale) {
    return Boolean(locale && LOCALE_MESSAGES[locale]);
}

export function normalizeAdminLocale(locale) {
    if (!locale) {
        return DEFAULT_ADMIN_LOCALE;
    }
    if (isSupportedAdminLocale(locale)) {
        return locale;
    }
    const matchedLocale = LOCALE_DEFINITIONS.find((localeDefinition) =>
        String(locale).toLowerCase().startsWith(localeDefinition.value.toLowerCase())
    );
    return matchedLocale ? matchedLocale.value : DEFAULT_ADMIN_LOCALE;
}

export function getStoredAdminLocale() {
    try {
        return normalizeAdminLocale(window.localStorage.getItem(ADMIN_LOCALE_STORAGE_KEY));
    } catch (e) {
        return DEFAULT_ADMIN_LOCALE;
    }
}

export function storeAdminLocale(locale) {
    try {
        window.localStorage.setItem(ADMIN_LOCALE_STORAGE_KEY, normalizeAdminLocale(locale));
    } catch (e) {
        // ignore
    }
}

export function translate(locale, key, params) {
    if (key === null || key === undefined) {
        return "";
    }
    const normalizedLocale = normalizeAdminLocale(locale);
    const source = String(key);
    const localeMessages = LOCALE_MESSAGES[normalizedLocale] || LOCALE_MESSAGES[DEFAULT_ADMIN_LOCALE] || {};
    const defaultMessages = LOCALE_MESSAGES[DEFAULT_ADMIN_LOCALE] || {};
    const translated = localeMessages[source] || defaultMessages[source] || source;
    return formatMessage(translated, params);
}

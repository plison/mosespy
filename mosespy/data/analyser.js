/*
 * Javascript code for the webpage used to analyse translation outputs
 * 
 */

function clean(stri) {
    return stri.replace(/&/g, "&amp;").replace(/>/g, "&gt;").replace(/</g, "&lt;").replace(/"/g,
            "&quot;").replace(/'/g, "&apos;");
}

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)");
    results = regex.exec(location.search);
    return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function isSatisfied(pair, conditions) {

    tokenised = (conditions['tokenised'] == "yes");
    punctuation = (conditions['punct'] == "yes");

    source = pair.find('source');
    nbsource = (punctuation) ? source.attr('nbtokens') : source.attr('nbwords');
    translation = pair.find('translation');
    nbtrans = (punctuation) ? translation.attr('nbtokens') : translation.attr('nbwords');
    if ((conditions['minsource'] != "" && nbsource < +conditions['minsource'])
            || (conditions['maxsource'] != "" && nbsource > +conditions['maxsource'])
            || (conditions['mintrans'] != "" && nbtrans < +conditions['mintrans'])
            || (conditions['maxtrans'] != "" && nbtrans > +conditions['maxtrans'])) {
        return false;
    }

    oneSatisfied = false;
    pair.find("reference").each(
            function(m) {
                nbref = (punctuation) ? $(this).attr('nbtokens') : $(this).attr('nbwords');
                wer = (punctuation) ? $(this).attr('wer_punct') : $(this).attr('wer');
                if ((conditions['minref'] == "" || nbref >= +conditions['minref'])
                        && (conditions['maxref'] == "" || nbref <= +conditions['maxref'])
                        && (conditions['minwer'] == "" || wer >= +conditions['minwer'])
                        && (conditions['maxwer'] == "" || wer <= +conditions['maxwer'])) {
                    oneSatisfied = true;
                }
            });
    if (!(oneSatisfied)) {
        return false;
    }

    if (conditions['sourcesub'] != "") {
        sourcetext = getContent(source, tokenised, punctuation);
        if (sourcetext.search(conditions['sourcesub']) == -1) {
            return false;
        }
    }

    if (conditions['refsub'] != "") {
        oneSatisfied = false;
        pair.find("reference").each(function(m) {
            reftext = getContent($(this), tokenised, punctuation);
            if (reftext.search(conditions['refsub']) != -1) {
                oneSatisfied = true;
            }
        });
        if (!(oneSatisfied)) {
            return false;
        }
    }

    if (conditions['transsub'] != "") {
        transtext = getContent(translation, tokenised, punctuation);
        if (transtext.search(conditions['transsub']) == -1) {
            return false;
        }
    }

    return true;
}

function reduce(tokens, contiguous) {
    cleaned = [];
    for (i = 0; i < tokens.length; i++) {
        if (i > 0 && contiguous.indexOf("" + i) < 0) {
            cleaned.push(" ");
        }
        cleaned.push(tokens[i]);
    }
    return cleaned.join("");
}

function getContent(xmlTag, tokenised, punctuation) {
    raw = xmlTag.text();
    if (tokenised) {
        return raw;
    }
    contiguous = xmlTag.attr('contiguous').split(",");
    tokens = raw.split(" ");
    return reduce(tokens, contiguous);
}

function getColourContent(xmlTag, tokenised, punctuation) {
    edits = (punctuation) ? xmlTag.attr('edits_punct').split("") : xmlTag.attr('edits').split("");
    if (typeof edits == typeof undefined) {
        return getContent(xmlTag, tokenised, punctuation);
    }
    tokens = xmlTag.text().split(" ");
    colour_tokens = [];
    for (j = 0; j < tokens.length; ++j) {
        token = tokens[j];
        edit = edits[j];
        colouredtok = (edit == 'p') ? token : "<span class='" + edit + "-colour'>" + token + "</span>";
        colour_tokens.push(colouredtok);
    }
    if (tokenised) {
        return colour_tokens.join(" ");
    }
    contiguous = xmlTag.attr('contiguous').split(",");
    return reduce(colour_tokens, contiguous);
}

function generateTable(conditions) {
    data = $('#transdata').html();
    xmlDoc = $.parseXML(data);
    $xml = $(xmlDoc);
    incr = 1;

    tokenised = (conditions['tokenised'] == "yes");
    punctuation = (conditions['punct'] == "yes");

    $xml.find("pair").each(function(index) {

        if (incr > conditions['tablesize']) {
            return false;
        }
        if (isSatisfied($(this), conditions) == false) {
            return true;
        }

        sourceTag = $(this).find('source');
        sourceText = getContent(sourceTag, tokenised, punctuation);

        translationTag = $(this).find('translation');
        translationText = getContent(translationTag, tokenised, punctuation);
        translationColour = getColourContent(translationTag, tokenised, punctuation);

        if (typeof (previous) !== 'undefined') {
            tooltip_source = "<b>Previous (source):</b> " + previous[0] + "<br>";
            tooltip_source += "<b>Previous (reference):</b> " + previous[1] + "<br>";
            tooltip_source += "<b>Previous (translation):</b> " + previous[2];
        } else {
            tooltip_source = "(None)";
        }

        tooltip_reference = "";
        $(this).find("reference").each(function(m) {
            referenceText = getContent($(this), tokenised, punctuation);
            if (typeof $(this).attr('edits') != typeof undefined) {
                fullRef = getColourContent($(this), tokenised, punctuation);
                closestRef = referenceText;
            } else {
                fullRef = referenceText;
            }
            wer = (punctuation) ? $(this).attr('wer_punct') : $(this).attr('wer');
            tooltip_reference += "<li>" + fullRef + " <b>(WER: " + wer + "&#37;)</b>" + "</li>";
        });

        line = "<tr><td>" + incr + "</td>";
        line += "<td><a href='#' tooltip-title='Contextual factors' tooltip-content='";
        line += clean(tooltip_source) + "'>" + sourceText + "</a></td>";
        line += "<td><a href='#' tooltip-title='Reference translations' tooltip-content='";
        line += clean(tooltip_reference) + "'>" + translationColour + "</td></tr>";

        $('.outputs').append(line);
        previous = [ sourceText, closestRef, translationText ];
        incr += 1;
    });
    $('#nboutputs').text((incr - 1));
}

function enableTooltips() {
    $('a[tooltip-content]').qtip({
        style : {
            classes : 'qtip-bootstrap'
        },
        position : {
            my : 'top left',
            at : 'bottom left'
        },
        content : {
            attr : 'tooltip-content',
            title : function(event, api) {
                return $(this).attr('tooltip-title');
            }
        }
    });
}

function getConditions() {
    params = [ "minsource", "maxsource", "minref", "maxref", "mintrans", "maxtrans", "minwer" ];
    params = params.concat([ "maxwer", "punct", "sourcesub", "refsub", "transsub", "tokenised","tablesize" ]);
    conditions = {};
    for (p = 0; p < params.length; p++) {
        param = params[p];
        value = getParameterByName(param);
        conditions[param] = value;
        if (value != "") {
            if (param == 'punct' || param == 'tokenised') {
                $("input[name=" + param + "]").prop('checked', true);
            } else {
                $("[name=" + param + "]").val(value);
            }
        }
    }
    if (conditions['tablesize'] == "") {
        conditions['tablesize'] = 100;
    }
    return conditions;
}

$(document).ready(function() {
    conditions = getConditions();
    conditions['full'] = false;
    generateTable(conditions);
    enableTooltips();
});   

# -*- coding: utf-8 -*-
# Regex to extract US street address
class Pattern_add:
    street1 = "Avenue|Way|Lane|Road|Boulevard|port|Drive|Street|Beach|Place|Box|pike|shore|Building|way|Row|Tower|Suite|Plaza|Square|sq|Trail|trl"
    street2 = "Ave|Dr|Rd|Blvd|Ln|St|hwy"
    street=street1+"|"+street1.upper()+"|"+street2
    state1 = "Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|New.?Hampshire|New.?Jersey|New.?Mexico|New.?York|North.?Carolina|North.?Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode.?Island|South.?Carolina|South.?Dakota|Tennessee|Texas|Utah|Vermont|Virginia|Washington|West.?Virginia|Wisconsin|Wyoming"
    state_abb="AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FM|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MH|MD|MA|MI|MN|MS|MO|MT|NE|NV|N\.?H|N\.?J|N\.?M|N\.?Y|N\.?C|N\.?D|MP|OH|OK|OR|PW|PA|PR|R\.?I|S\.?C|S\.?D|TN|TX|UT|VT|VI|VA|WA|W\.?V|WI|WY"
    state=state1+"|"+state1.upper()
    num1='One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|P\.?O'
    num=num1+"|"+num1.upper()
    p_full="((\d{1,5}|"+num+")([^0-9\:\\[\\(\\)\|]{1,20})("+street+")([^\:\\(\\)\|]{1,30})("+state+"))"
    p_abb="((\d{1,5}|"+num+")([^0-9\:\\[\\(\\)\|]{1,20})("+street+")([^\:\\(\\)\|]{1,30})("+state_abb+"))"

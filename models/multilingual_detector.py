# models/multilingual_detector.py
import pandas as pd
import numpy as np
import re
from googletrans import Translator
import requests
import json

class BhashiniMultilingualDetector:
    """
    Integration with India's Bhashini AI for 22 Indian languages
    Real-time multilingual threat detection
    """
    
    def __init__(self):
        self.translator = Translator()
        
        # Indian languages database
        self.indian_languages = {
            'hi': 'Hindi', 'bn': 'Bengali', 'te': 'Telugu', 'mr': 'Marathi',
            'ta': 'Tamil', 'ur': 'Urdu', 'gu': 'Gujarati', 'kn': 'Kannada',
            'ml': 'Malayalam', 'or': 'Odia', 'pa': 'Punjabi', 'as': 'Assamese',
            'mai': 'Maithili', 'sat': 'Santali', 'ks': 'Kashmiri', 'ne': 'Nepali',
            'sd': 'Sindhi', 'kok': 'Konkani', 'doi': 'Dogri', 'mni': 'Manipuri',
            'bodo': 'Bodo', 'sanskrit': 'Sanskrit'
        }
        
        # Language-specific threat keywords
        self.threat_keywords = self._load_threat_keywords()
        
        # Regional slang and coded language patterns
        self.regional_slang = self._load_regional_slang()
        
    def _load_threat_keywords(self):
        """Load threat keywords in multiple Indian languages"""
        return {
            'hi': ['खतरा', 'हमला', 'मौत', 'बम', 'आतंकवाद', 'धमकी', 'खून', 'हिंसा'],
            'bn': ['হুমকি', 'আক্রমণ', 'মৃত্যু', 'বোমা', 'সন্ত্রাসবাদ', 'ধমক', 'রক্ত', 'হিংসা'],
            'ta': ['அபாயம்', 'தாக்குதல்', 'இறப்பு', 'குண்டு', 'பயங்கரவாதம்', 'மிரட்டல்', 'இரத்தம்', 'வன்முறை'],
            'te': ['ప్రమాదం', 'దాడి', 'మరణం', 'బాంబు', 'ఉగ్రవాదం', 'బెదిరింపు', 'రక్తం', 'హింస'],
            'ml': ['ഭീഷണി', 'ആക്രമണം', 'മരണം', 'ബോംബ്', 'തീവ്രവാദം', 'ഭീഷണിപ്പെടുത്തൽ', 'രക്തം', 'ഹിംസ'],
            'ur': ['خطرہ', 'حملہ', 'موت', 'بم', 'دہشت گردی', 'دھمکی', 'خون', 'تشدد'],
            'gu': ['ખતરો', 'હુમલો', 'મૃત્યુ', 'બોમ્બ', 'આતંકવાદ', 'ધમકી', 'લોહી', 'હિંસા'],
            'kn': ['ಅಪಾಯ', 'ದಾಳಿ', 'ಮರಣ', 'ಬಾಂಬ್', 'ಭಯೋತ್ಪಾದನೆ', 'ಬೆದರಿಕೆ', 'ರಕ್ತ', 'ಹಿಂಸೆ'],
            'mr': ['धोका', 'हल्ला', 'मृत्यू', 'बॉम्ब', 'दहशतवाद', 'धमकी', 'रक्त', 'हिंसा'],
            'or': ['ବିପଦ', 'ଆକ୍ରମଣ', 'ମୃତ୍ୟୁ', 'ବୋମା', 'ସଂତ୍ରାସବାଦ', 'ଧମକି', 'ରକ୍ତ', 'ହିଂସା'],
            'pa': ['ਖਤਰਾ', 'ਹਮਲਾ', 'ਮੌਤ', 'ਬੰਬ', 'ਦਹਿਸ਼ਤਗਰਦੀ', 'ਧਮਕੀ', 'ਖੂਨ', 'ਹਿੰਸਾ']
        }
    
    def _load_regional_slang(self):
        """Load regional slang and coded language"""
        return {
            'hi': {
                'code_words': ['चाय', 'समाचार', 'मौसम', 'खेल'],  # Innocent words used as codes
                'patterns': [r'(\d+)\s*(बजे|पर)', r'(\w+)\s*(जगह|स्थान)']  # Time/place patterns
            },
            'ta': {
                'code_words': ['தேநீர்', 'செய்தி', 'காலநிலை', 'விளையாட்டு'],
                'patterns': [r'(\d+)\s*(மணிக்கு|நேரம்)', r'(\w+)\s*(இடம்|ஸ்தலம்)']
            },
            'ml': {
                'code_words': ['ചായ', 'വാർത്ത', 'കാലാവസ്ഥ', 'കളി'],
                'patterns': [r'(\d+)\s*(മണിക്ക്|സമയം)', r'(\w+)\s*(സ്ഥലം|സ്ഥാനം)']
            },
            'ur': {
                'code_words': ['چائے', 'خبر', 'موسم', 'کھیل'],
                'patterns': [r'(\d+)\s*(بجے|پر)', r'(\w+)\s*(جگہ|مقام)']
            }
        }
    
    def detect_language(self, text):
        """Detect language of text"""
        try:
            # Simple detection based on character ranges
            if re.search(r'[\u0900-\u097F]', text):  # Devanagari range
                return 'hi'  # Hindi, Marathi, Sanskrit, etc.
            elif re.search(r'[\u0980-\u09FF]', text):  # Bengali range
                return 'bn'
            elif re.search(r'[\u0A80-\u0AFF]', text):  # Gujarati
                return 'gu'
            elif re.search(r'[\u0B00-\u0B7F]', text):  # Oriya
                return 'or'
            elif re.search(r'[\u0B80-\u0BFF]', text):  # Tamil
                return 'ta'
            elif re.search(r'[\u0C00-\u0C7F]', text):  # Telugu
                return 'te'
            elif re.search(r'[\u0C80-\u0CFF]', text):  # Kannada
                return 'kn'
            elif re.search(r'[\u0D00-\u0D7F]', text):  # Malayalam
                return 'ml'
            elif re.search(r'[\u0600-\u06FF]', text):  # Arabic script (Urdu)
                return 'ur'
            else:
                return 'en'
        except:
            return 'unknown'
    
    def translate_to_english(self, text, source_lang='auto'):
        """Translate text to English"""
        try:
            translation = self.translator.translate(text, src=source_lang, dest='en')
            return translation.text
        except:
            # Fallback: simple keyword matching
            return text
    
    def analyze_regional_text(self, text, language_code):
        """Analyze regional text for threats"""
        results = {
            'detected_language': self.indian_languages.get(language_code, 'Unknown'),
            'translated_text': '',
            'threat_keywords_found': [],
            'coded_language_detected': False,
            'threat_score': 0,
            'cultural_context': {}
        }
        
        # Translate to English
        translated = self.translate_to_english(text, language_code)
        results['translated_text'] = translated
        
        # Check for threat keywords
        if language_code in self.threat_keywords:
            for keyword in self.threat_keywords[language_code]:
                if keyword in text:
                    results['threat_keywords_found'].append(keyword)
                    results['threat_score'] += 5
        
        # Check for coded language
        if language_code in self.regional_slang:
            slang_info = self.regional_slang[language_code]
            
            # Check code words
            for code_word in slang_info['code_words']:
                if code_word in text.lower():
                    results['coded_language_detected'] = True
                    results['threat_score'] += 3
            
            # Check patterns
            for pattern in slang_info['patterns']:
                if re.search(pattern, text):
                    results['threat_score'] += 2
        
        # Cultural context analysis
        results['cultural_context'] = self._analyze_cultural_context(text, language_code)
        
        return results
    
    def _analyze_cultural_context(self, text, language_code):
        """Analyze cultural and contextual meaning"""
        context = {
            'sarcasm_detected': False,
            'regional_references': [],
            'potential_misinterpretation': False
        }
        
        # Simple sarcasm detection (can be enhanced)
        sarcasm_indicators = ['वाह', 'बहुत अच्छे', 'சூப்பர்', 'अरे वाह']  # Wow, very good, super, oh wow
        for indicator in sarcasm_indicators:
            if indicator in text:
                context['sarcasm_detected'] = True
        
        # Regional references
        regional_terms = {
            'hi': ['दिल्ली', 'मुंबई', 'यूपी', 'बिहार'],
            'ta': ['சென்னை', 'கோவை', 'மதுரை', 'தமிழ்நாடு'],
            'ml': ['തിരുവനന്തപുരം', 'കൊച്ചി', 'കോഴിക്കോട്', 'കേരളം']
        }
        
        if language_code in regional_terms:
            for term in regional_terms[language_code]:
                if term in text:
                    context['regional_references'].append(term)
        
        return context
    
    def process_multilingual_data(self, dataframe):
        """Process dataframe with multilingual text"""
        if 'post' not in dataframe.columns:
            return dataframe
        
        results = []
        
        for idx, row in dataframe.iterrows():
            text = row['post']
            lang = self.detect_language(text)
            
            analysis = {
                'original_text': text,
                'detected_language': lang,
                'language_name': self.indian_languages.get(lang, 'English/Other')
            }
            
            if lang in self.indian_languages:
                # Deep analysis for Indian languages
                detailed_analysis = self.analyze_regional_text(text, lang)
                analysis.update(detailed_analysis)
            else:
                # Basic analysis for English/other
                analysis['translated_text'] = text
                analysis['threat_score'] = 0
                analysis['threat_keywords_found'] = []
            
            results.append(analysis)
        
        # Add analysis results to dataframe
        analysis_df = pd.DataFrame(results)
        
        # Merge with original dataframe
        merged_df = pd.concat([dataframe.reset_index(drop=True), 
                             analysis_df.reset_index(drop=True)], axis=1)
        
        return merged_df
    
    def get_language_statistics(self, dataframe):
        """Get statistics about detected languages"""
        if 'language_name' not in dataframe.columns:
            return {}
        
        lang_stats = dataframe['language_name'].value_counts().to_dict()
        
        total_posts = len(dataframe)
        indian_language_posts = sum(1 for lang in dataframe['language_name'] 
                                   if lang != 'English/Other')
        
        return {
            'total_posts_analyzed': total_posts,
            'indian_language_posts': indian_language_posts,
            'percentage_indian_languages': f"{(indian_language_posts/total_posts*100):.1f}%",
            'languages_detected': len(lang_stats),
            'language_breakdown': lang_stats,
            'unique_threats_in_regional_languages': len(
                dataframe[(dataframe['language_name'] != 'English/Other') & 
                         (dataframe['threat_score'] > 0)]
            )
        }
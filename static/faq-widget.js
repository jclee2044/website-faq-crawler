(function() {
  'use strict';

  class FAQWidget extends HTMLElement {
    static get observedAttributes() {
      return ['data-api', 'data-url', 'data-lang', 'data-heading', 'data-jsonld', 'data-config'];
    }

    constructor() {
      super();
      this.retryCount = 0;
      this.maxRetries = 3;
      this.retryDelay = 2000;
      this.requestTimeout = 8000;
      this.maxFaqs = 10;
      this.maxInlineItems = 100;
      this.maxInlineSize = 64 * 1024; // 64KB
      this.currentOpenIndex = -1;
    }

    connectedCallback() {
      this.initializeWidget();
    }

    attributeChangedCallback(name, oldValue, newValue) {
      if (name === 'data-config') {
        const config = this.parseConfig(newValue || '');
        this.applyConfig(config);
        return;
      }
      if (this.shadowRoot) {
        this.initializeWidget();
      }
    }

    initializeWidget() {
      // Parse attributes
      const api = this.getAttribute('data-api') || 'http://localhost:8000';
      const url = this.getAttribute('data-url');
      const lang = this.getAttribute('data-lang');
      const heading = this.getAttribute('data-heading') || 'Frequently Asked Questions';
      const jsonld = this.getAttribute('data-jsonld') !== 'off';
      const configStr = this.getAttribute('data-config') || '';
      const config = this.parseConfig(configStr);

      // Attach shadow DOM
      if (!this.shadowRoot) {
        this.attachShadow({ mode: 'open' });
      }

      // Apply styling config via CSS variables on host
      this.applyConfig(config);

      // Check for inline JSON first
      const inlineJson = this.getInlineJson();
      if (inlineJson) {
        this.render(inlineJson, heading);
        if (jsonld) {
          this.injectJsonLd(inlineJson, heading);
        }
        return;
      }

      // No inline JSON, try API if URL provided
      if (url) {
        this.renderLoading(heading);
        this.loadFaqsWithRetry(api, url, lang, heading, jsonld);
      } else {
        this.renderError('No URL provided and no inline FAQs found');
      }
    }

    getInlineJson() {
      const script = this.querySelector('script[type="application/json"]');
      if (!script) return null;

      try {
        const text = script.textContent || script.innerText || '';
        
        // Size check
        if (text.length > this.maxInlineSize) {
          console.warn('FAQ Widget: Inline JSON too large, ignoring');
          return null;
        }

        const data = JSON.parse(text);
        
        // Validate and limit items
        if (!Array.isArray(data)) return null;
        if (data.length > this.maxInlineItems) {
          console.warn('FAQ Widget: Too many inline items, truncating');
          data.splice(this.maxInlineItems);
        }

        return data;
      } catch (e) {
        console.warn('FAQ Widget: Invalid inline JSON, falling back to API');
        return null;
      }
    }

    parseConfig(configStr) {
      if (!configStr) return {};
      try {
        const parsed = JSON.parse(configStr);
        return parsed && typeof parsed === 'object' ? parsed : {};
      } catch (e) {
        console.warn('FAQ Widget: Invalid data-config JSON, ignoring');
        return {};
      }
    }

    applyConfig(config) {
      if (!config || typeof config !== 'object') return;
      const mappings = {
        font_family: '--faq-font-family',
        header_color: '--faq-header-color',
        header_background_color: '--faq-header-bg',
        title_font_weight: '--faq-title-weight',
        title_font_size: '--faq-title-size',
        body_background_color: '--faq-body-bg',
        message_font_size: '--faq-message-size'
      };
      Object.keys(mappings).forEach(key => {
        const cssVar = mappings[key];
        const value = config[key];
        if (typeof value === 'string' && value.trim() !== '') {
          this.style.setProperty(cssVar, value);
        }
      });
    }

    async loadFaqsWithRetry(api, url, lang, heading, jsonld) {
      this.retryCount = 0;
      
      while (this.retryCount < this.maxRetries) {
        try {
          const result = await this.fetchFaqs(api, url, lang);
          
          if (result && result.faqs && result.faqs.length > 0) {
            this.render(result.faqs, heading);
            if (jsonld) {
              this.injectJsonLd(result.faqs, heading);
            }
            return;
          }

          // Check if we should retry
          const shouldRetry = result && (
            result.faq_file ||
            result.just_crawled ||
            result.faq_generated ||
            (result.message && result.message.includes('generating'))
          );

          if (!shouldRetry) {
            this.renderError('No FAQs available for this page');
            return;
          }

          // Try force refresh if we have a FAQ file but empty results
          if (result.faq_file && (!result.faqs || result.faqs.length === 0)) {
            const refreshResult = await this.fetchFaqs(api, url, lang, true);
            if (refreshResult && refreshResult.faqs && refreshResult.faqs.length > 0) {
              this.render(refreshResult.faqs, heading);
              if (jsonld) {
                this.injectJsonLd(refreshResult.faqs, heading);
              }
              return;
            }
          }

          this.retryCount++;
          if (this.retryCount < this.maxRetries) {
            this.renderStatus(`Generating FAQs... retrying (${this.retryCount}/${this.maxRetries})`);
            await this.delay(this.retryDelay);
          }
        } catch (error) {
          this.retryCount++;
          if (this.retryCount >= this.maxRetries) {
            this.renderError(`Failed to load FAQs: ${error.message}`);
            return;
          }
          await this.delay(this.retryDelay);
        }
      }

      this.renderError('No FAQs generated after multiple attempts');
    }

    async fetchFaqs(api, url, lang, forceRefresh = false) {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.requestTimeout);

      try {
        const params = new URLSearchParams();
        params.set('url', url);
        if (lang) params.set('target_language', lang);
        if (forceRefresh) params.set('force_refresh', 'true');

        const response = await fetch(`${api}/page-faqs?${params}`, {
          mode: 'cors',
          credentials: 'omit',
          headers: { 'Accept': 'application/json' },
          signal: controller.signal
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        return await response.json();
      } catch (error) {
        clearTimeout(timeoutId);
        if (error.name === 'AbortError') {
          throw new Error('Request timeout');
        }
        throw error;
      }
    }

    render(faqs, heading) {
      const validFaqs = this.validateAndLimitFaqs(faqs);
      
      this.shadowRoot.innerHTML = `
        <style>
          :host {
            display: block;
            font-family: var(--faq-font-family, system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif);
            font-size: 16px;
            line-height: 1.5;
            color: #333;
            max-width: 100%;
          }
          
          .faq-container {
            border-radius: 8px;
            overflow: hidden;
            background: var(--faq-body-bg, transparent);
          }
          
          .faq-header {
            background: var(--faq-header-bg, transparent);
          }
          
          .faq-heading {
            font-size: var(--faq-title-size, 24px);
            font-weight: var(--faq-title-weight, 600);
            margin: 0 0 16px 0;
            color: var(--faq-header-color, #111);
          }
          
          .faq-item {
            border: 1px solid #e5e7eb;
            border-bottom: none;
          }
          
          .faq-item:last-child {
            border-bottom: 1px solid #e5e7eb;
          }
          
          .faq-question {
            width: 100%;
            padding: 16px;
            background: #fff;
            border: none;
            text-align: left;
            font-size: var(--faq-message-size, 16px);
            font-weight: 500;
            color: #111;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: background-color 0.2s;
          }
          
          .faq-question:hover {
            background: #f9fafb;
          }
          
          .faq-question:focus {
            outline: 2px solid #3b82f6;
            outline-offset: -2px;
          }
          
          .faq-question[aria-expanded="true"] {
            background: #f3f4f6;
          }
          
          .faq-icon {
            font-size: 18px;
            transition: transform 0.2s;
            color: #6b7280;
          }
          
          .faq-question[aria-expanded="true"] .faq-icon {
            transform: rotate(180deg);
          }
          
          .faq-answer {
            padding: 0 16px;
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease, padding 0.3s ease;
            background: #f9fafb;
          }
          
          .faq-answer[aria-hidden="false"] {
            padding: 16px;
            max-height: 500px;
          }
          
          .faq-answer-content {
            color: #374151;
            margin: 0;
            font-size: var(--faq-message-size, 16px);
          }
          
          .status {
            padding: 12px 16px;
            background: #f3f4f6;
            border-radius: 6px;
            font-size: 14px;
            color: #6b7280;
            text-align: center;
          }
          
          .error {
            background: #fef2f2;
            color: #dc2626;
          }
        </style>
        
        <div class="faq-container" aria-busy="false">
          <div class="faq-header">
            <h2 class="faq-heading">${this.escapeHtml(heading)}</h2>
          </div>
          ${validFaqs.length > 0 ? this.buildAccordion(validFaqs) : '<div class="status">No FAQs available</div>'}
        </div>
      `;

      this.setupAccordionBehavior();
    }

    buildAccordion(faqs) {
      return faqs.map((faq, index) => `
        <div class="faq-item">
          <button 
            class="faq-question" 
            aria-expanded="false" 
            aria-controls="faq-answer-${index}"
            data-index="${index}"
          >
            <span>${this.escapeHtml(faq.question)}</span>
            <span class="faq-icon" aria-hidden="true">▼</span>
          </button>
          <div 
            class="faq-answer" 
            id="faq-answer-${index}"
            role="region" 
            aria-labelledby="faq-question-${index}"
            aria-hidden="true"
          >
            <p class="faq-answer-content">${this.escapeHtml(faq.answer)}</p>
          </div>
        </div>
      `).join('');
    }

    setupAccordionBehavior() {
      const questions = this.shadowRoot.querySelectorAll('.faq-question');
      
      questions.forEach((question, index) => {
        question.addEventListener('click', () => this.toggleItem(index));
        question.addEventListener('keydown', (e) => this.handleKeydown(e, index));
      });
    }

    toggleItem(index) {
      const questions = this.shadowRoot.querySelectorAll('.faq-question');
      const answers = this.shadowRoot.querySelectorAll('.faq-answer');
      
      // Close current item if it's open
      if (this.currentOpenIndex === index) {
        this.closeItem(index, questions, answers);
        this.currentOpenIndex = -1;
      } else {
        // Close previous item if any
        if (this.currentOpenIndex >= 0) {
          this.closeItem(this.currentOpenIndex, questions, answers);
        }
        
        // Open new item
        this.openItem(index, questions, answers);
        this.currentOpenIndex = index;
      }
    }

    openItem(index, questions, answers) {
      const question = questions[index];
      const answer = answers[index];
      
      question.setAttribute('aria-expanded', 'true');
      answer.setAttribute('aria-hidden', 'false');
    }

    closeItem(index, questions, answers) {
      const question = questions[index];
      const answer = answers[index];
      
      question.setAttribute('aria-expanded', 'false');
      answer.setAttribute('aria-hidden', 'true');
    }

    handleKeydown(event, currentIndex) {
      const questions = this.shadowRoot.querySelectorAll('.faq-question');
      
      switch (event.key) {
        case 'Enter':
        case ' ':
          event.preventDefault();
          this.toggleItem(currentIndex);
          break;
        case 'ArrowDown':
          event.preventDefault();
          const nextIndex = (currentIndex + 1) % questions.length;
          questions[nextIndex].focus();
          break;
        case 'ArrowUp':
          event.preventDefault();
          const prevIndex = currentIndex === 0 ? questions.length - 1 : currentIndex - 1;
          questions[prevIndex].focus();
          break;
      }
    }

    renderLoading(heading) {
      this.shadowRoot.innerHTML = `
        <style>
          :host {
            display: block;
            font-family: var(--faq-font-family, system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif);
            font-size: 16px;
            line-height: 1.5;
            color: #333;
          }
          
          .faq-container {
            border-radius: 8px;
            overflow: hidden;
            background: var(--faq-body-bg, transparent);
          }
          
          .faq-header {
            background: var(--faq-header-bg, transparent);
          }
          
          .faq-heading {
            font-size: var(--faq-title-size, 24px);
            font-weight: var(--faq-title-weight, 600);
            margin: 0 0 16px 0;
            color: var(--faq-header-color, #111);
          }
          
          .status {
            padding: 12px 16px;
            background: #f3f4f6;
            border-radius: 6px;
            font-size: 14px;
            color: #6b7280;
            text-align: center;
          }
        </style>
        
        <div class="faq-container" aria-busy="true">
          <div class="faq-header">
            <h2 class="faq-heading">${this.escapeHtml(heading)}</h2>
          </div>
          <div class="status">Loading FAQs...</div>
        </div>
      `;
    }

    renderStatus(message) {
      const statusEl = this.shadowRoot.querySelector('.status');
      if (statusEl) {
        statusEl.textContent = message;
      }
    }

    renderError(message) {
      this.shadowRoot.innerHTML = `
        <style>
          :host {
            display: block;
            font-family: var(--faq-font-family, system-ui, -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif);
            font-size: 16px;
            line-height: 1.5;
            color: #333;
          }
          
          .status {
            padding: 12px 16px;
            background: #fef2f2;
            border-radius: 6px;
            font-size: 14px;
            color: #dc2626;
            text-align: center;
          }
        </style>
        
        <div class="status error">${this.escapeHtml(message)}</div>
      `;
    }

    validateAndLimitFaqs(faqs) {
      if (!Array.isArray(faqs)) return [];
      
      return faqs
        .filter(faq => faq && typeof faq.question === 'string' && typeof faq.answer === 'string')
        .map(faq => ({
          question: faq.question.trim(),
          answer: faq.answer.trim()
        }))
        .filter(faq => faq.question && faq.answer)
        .slice(0, this.maxFaqs);
    }

    injectJsonLd(faqs, heading) {
      const validFaqs = this.validateAndLimitFaqs(faqs);
      if (validFaqs.length === 0) return;

      const jsonLd = {
        '@context': 'https://schema.org',
        '@type': 'FAQPage',
        'name': heading,
        'mainEntity': validFaqs.map(faq => ({
          '@type': 'Question',
          'name': faq.question,
          'acceptedAnswer': {
            '@type': 'Answer',
            'text': faq.answer
          }
        }))
      };

      // Remove existing JSON-LD if any
      const existing = this.querySelector('script[type="application/ld+json"]');
      if (existing) {
        existing.remove();
      }

      // Add new JSON-LD to light DOM
      const script = document.createElement('script');
      script.type = 'application/ld+json';
      script.textContent = JSON.stringify(jsonLd);
      this.appendChild(script);
    }

    escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    delay(ms) {
      return new Promise(resolve => setTimeout(resolve, ms));
    }
  }

  // Register the custom element
  if (!customElements.get('faq-widget')) {
    customElements.define('faq-widget', FAQWidget);

  // Auto-mount: create and insert <faq-widget> from script data attributes
  (function() {
    const s = document.currentScript;
    if (!s || !s.dataset) return;
    const targetSel = s.dataset.target;
    if (!targetSel) return;
    const mountPoint = document.querySelector(targetSel);
    if (!mountPoint) return;
    const el = document.createElement('faq-widget');
    if (s.dataset.api) el.setAttribute('data-api', s.dataset.api);
    if (s.dataset.url) el.setAttribute('data-url', s.dataset.url);
    if (s.dataset.lang) el.setAttribute('data-lang', s.dataset.lang);
    if (s.dataset.heading) el.setAttribute('data-heading', s.dataset.heading);
    if (s.dataset.jsonld === 'off') el.setAttribute('data-jsonld', 'off');
    if (s.dataset.max && !Number.isNaN(Number(s.dataset.max))) {
      el.setAttribute('data-max', String(Math.max(1, Number(s.dataset.max))));
    }
    if (s.dataset.config) {
      el.setAttribute('data-config', s.dataset.config);
    }
    mountPoint.appendChild(el);
  })();

  }
})();

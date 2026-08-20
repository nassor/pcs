/* ==========================================================================
   PCS Documentation — behaviour
   No framework. Every block here is a small, independent enhancement; the
   site is fully usable with JavaScript disabled.
   ========================================================================== */

(() => {
  'use strict';

  const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  /** Normalise a pathname for comparison: strip trailing slash and index.html. */
  function normalisePath(pathname) {
    return pathname.replace(/index\.html$/, '').replace(/\/+$/, '') || '/';
  }

  /** Same-origin check that survives absolute internal URLs. */
  function isExternal(anchor) {
    // `anchor.href` is always absolute and already resolved by the browser.
    return anchor.origin !== window.location.origin;
  }

  document.addEventListener('DOMContentLoaded', () => {
    const sidebar = document.querySelector('.sidebar');
    const menuToggle = document.getElementById('menu-toggle');
    const overlay = document.querySelector('.sidebar-overlay');

    // ------------------------------------------------------------------------
    // External links open in a new tab. Internal ones must not.
    //
    // Zola's `get_url` emits absolute URLs because `base_url` is absolute, so
    // a naive `a[href^="http"]` selector matches every internal nav link and
    // sends the whole site to new tabs. Compare origins instead.
    // ------------------------------------------------------------------------
    document.querySelectorAll('a[href]').forEach((link) => {
      if (link.hasAttribute('target')) return;
      if (!isExternal(link)) return;
      link.setAttribute('target', '_blank');
      link.setAttribute('rel', 'noopener noreferrer');
      link.classList.add('is-external');
    });

    // ------------------------------------------------------------------------
    // Active nav link
    //
    // Compare normalised pathnames, not raw href strings: the hrefs are
    // absolute and the current URL may or may not carry a trailing slash.
    // ------------------------------------------------------------------------
    const here = normalisePath(window.location.pathname);
    let activeLink = null;

    document.querySelectorAll('.nav-links a').forEach((link) => {
      if (isExternal(link)) return;
      const target = normalisePath(link.pathname);
      if (target === here) {
        link.classList.add('active');
        link.setAttribute('aria-current', 'page');
        activeLink = link;
      }
    });

    // The sidebar links to leaf pages, so a section landing page like
    // /benchmarks/ matches nothing above and the nav goes blank. Fall back to
    // the first link that lives under the current directory, and mark it as an
    // ancestor rather than the page itself.
    if (!activeLink && here !== '/') {
      const prefix = here.endsWith('/') ? here : here + '/';
      for (const link of document.querySelectorAll('.nav-links a')) {
        if (isExternal(link)) continue;
        if (normalisePath(link.pathname).startsWith(prefix)) {
          link.classList.add('active');
          link.setAttribute('aria-current', 'true');
          activeLink = link;
          break;
        }
      }
    }

    // Mark the enclosing group so its label can highlight too.
    if (activeLink) {
      const group = activeLink.closest('.nav-group');
      if (group) group.classList.add('has-active');
    }

    // ------------------------------------------------------------------------
    // Keep the sidebar's scroll position across navigations.
    //
    // The site is server-rendered, so each click is a real page load. Restoring
    // scroll makes the nav feel like a persistent shell rather than a page that
    // jumps back to the top every time.
    // ------------------------------------------------------------------------
    if (sidebar) {
      const KEY = 'pcs:sidebar-scroll';
      const saved = sessionStorage.getItem(KEY);
      if (saved !== null) sidebar.scrollTop = parseInt(saved, 10) || 0;

      // If the active link ended up off-screen, bring it into view instead.
      if (activeLink) {
        const linkBox = activeLink.getBoundingClientRect();
        const navBox = sidebar.getBoundingClientRect();
        if (linkBox.top < navBox.top || linkBox.bottom > navBox.bottom) {
          activeLink.scrollIntoView({ block: 'center' });
        }
      }

      window.addEventListener('beforeunload', () => {
        sessionStorage.setItem(KEY, String(sidebar.scrollTop));
      });
    }

    // ------------------------------------------------------------------------
    // Prefetch documentation pages on intent, so navigation feels immediate.
    // ------------------------------------------------------------------------
    if (!navigator.connection || !navigator.connection.saveData) {
      const prefetched = new Set([normalisePath(window.location.pathname)]);

      const prefetch = (href) => {
        const path = normalisePath(new URL(href).pathname);
        if (prefetched.has(path)) return;
        prefetched.add(path);
        const hint = document.createElement('link');
        hint.rel = 'prefetch';
        hint.href = href;
        document.head.appendChild(hint);
      };

      document.querySelectorAll('.sidebar a, .next-card, .concept-card').forEach((link) => {
        if (!link.href || isExternal(link)) return;
        link.addEventListener('pointerenter', () => prefetch(link.href), { once: true });
        link.addEventListener('focus', () => prefetch(link.href), { once: true });
      });
    }

    // ------------------------------------------------------------------------
    // Mobile navigation drawer
    // ------------------------------------------------------------------------
    function setDrawer(open) {
      if (!sidebar) return;
      sidebar.classList.toggle('open', open);
      if (overlay) overlay.classList.toggle('visible', open);
      if (menuToggle) menuToggle.setAttribute('aria-expanded', String(open));
      document.body.classList.toggle('nav-open', open);
    }

    if (menuToggle) {
      menuToggle.addEventListener('click', (e) => {
        e.stopPropagation();
        setDrawer(!sidebar.classList.contains('open'));
      });
    }

    if (overlay) overlay.addEventListener('click', () => setDrawer(false));

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') setDrawer(false);
    });

    // Following a link inside the drawer should close it.
    if (sidebar) {
      sidebar.addEventListener('click', (e) => {
        if (e.target.closest('a')) setDrawer(false);
      });
    }

    // ------------------------------------------------------------------------
    // On-page contents: highlight the section currently in view
    // ------------------------------------------------------------------------
    const tocLinks = Array.from(document.querySelectorAll('.page-toc a[href^="#"]'));
    if (tocLinks.length > 0 && 'IntersectionObserver' in window) {
      const byId = new Map();
      tocLinks.forEach((link) => {
        const target = document.getElementById(decodeURIComponent(link.hash.slice(1)));
        if (target) byId.set(target, link);
      });

      const spy = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            const link = byId.get(entry.target);
            if (!link) return;
            if (entry.isIntersecting) {
              tocLinks.forEach((l) => l.classList.remove('reading'));
              link.classList.add('reading');
            }
          });
        },
        { rootMargin: '-80px 0px -70% 0px' }
      );

      byId.forEach((_link, target) => spy.observe(target));
    }

    // ------------------------------------------------------------------------
    // Reveal-on-scroll for elements marked `.animate-in`
    // ------------------------------------------------------------------------
    const revealTargets = document.querySelectorAll('.animate-in');
    if (revealTargets.length > 0) {
      if (reduceMotion || !('IntersectionObserver' in window)) {
        revealTargets.forEach((el) => el.classList.add('revealed'));
      } else {
        const reveal = new IntersectionObserver(
          (entries) => {
            entries.forEach((entry) => {
              if (!entry.isIntersecting) return;
              entry.target.classList.add('revealed');
              reveal.unobserve(entry.target);
            });
          },
          { threshold: 0.05, rootMargin: '0px 0px -32px 0px' }
        );
        revealTargets.forEach((el) => reveal.observe(el));
      }
    }

    // ------------------------------------------------------------------------
    // Back to top
    // ------------------------------------------------------------------------
    const backToTop = document.querySelector('.back-to-top');
    if (backToTop) {
      const main = document.querySelector('.main-content') || document.documentElement;
      const onScroll = () => {
        backToTop.classList.toggle('visible', window.scrollY > 600);
      };
      onScroll();
      window.addEventListener('scroll', onScroll, { passive: true });
      backToTop.addEventListener('click', () => {
        window.scrollTo({ top: 0, behavior: reduceMotion ? 'auto' : 'smooth' });
        const heading = main.querySelector('h1');
        if (heading) heading.focus?.();
      });
    }

    // ------------------------------------------------------------------------
    // Copy button on code blocks
    // ------------------------------------------------------------------------
    document.querySelectorAll('pre').forEach((pre) => {
      const code = pre.querySelector('code');
      if (!code) return;

      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'copy-btn';
      button.textContent = 'Copy';
      button.setAttribute('aria-label', 'Copy code to clipboard');

      button.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(code.innerText);
          button.textContent = 'Copied';
          button.classList.add('copied');
        } catch {
          button.textContent = 'Press Ctrl+C';
        }
        setTimeout(() => {
          button.textContent = 'Copy';
          button.classList.remove('copied');
        }, 1800);
      });

      pre.appendChild(button);
    });
  });
})();

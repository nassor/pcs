document.addEventListener('DOMContentLoaded', () => {
  // --------------------------------------------------------------------------
  // Mermaid initialization
  // --------------------------------------------------------------------------
  if (window.mermaid) {
    mermaid.initialize({
      startOnLoad: true,
      theme: 'dark',
      securityLevel: 'loose',
      fontFamily: 'Outfit, sans-serif'
    });
  }

  // --------------------------------------------------------------------------
  // DOM references
  // --------------------------------------------------------------------------
  const menuToggle = document.getElementById('menu-toggle');
  const sidebar = document.querySelector('.sidebar');
  const overlay = document.querySelector('.sidebar-overlay');

  // --------------------------------------------------------------------------
  // Mobile menu toggle
  // --------------------------------------------------------------------------
  function openSidebar() {
    if (!sidebar) return;
    sidebar.classList.add('open');
    if (overlay) overlay.classList.add('visible');
    if (menuToggle) menuToggle.setAttribute('aria-expanded', 'true');
    if (menuToggle) menuToggle.innerHTML = '&#10005;';
  }

  function closeSidebar() {
    if (!sidebar) return;
    sidebar.classList.remove('open');
    if (overlay) overlay.classList.remove('visible');
    if (menuToggle) menuToggle.setAttribute('aria-expanded', 'false');
    if (menuToggle) menuToggle.innerHTML = '&#9776;';
  }

  if (menuToggle) {
    menuToggle.addEventListener('click', () => {
      if (sidebar && sidebar.classList.contains('open')) {
        closeSidebar();
      } else {
        openSidebar();
      }
    });
  }

  // Close sidebar when clicking overlay
  if (overlay) {
    overlay.addEventListener('click', closeSidebar);
  }

  // Close sidebar when clicking outside on mobile (fallback if no overlay)
  document.addEventListener('click', (e) => {
    if (window.innerWidth <= 768 && sidebar && sidebar.classList.contains('open')) {
      if (!sidebar.contains(e.target) && menuToggle && !menuToggle.contains(e.target)) {
        closeSidebar();
      }
    }
  });

  // Close sidebar on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && sidebar && sidebar.classList.contains('open')) {
      closeSidebar();
      if (menuToggle) menuToggle.focus();
    }
  });

  // --------------------------------------------------------------------------
  // Active nav link highlighting
  // --------------------------------------------------------------------------
  const currentPath = window.location.pathname;
  const currentPage = currentPath.substring(currentPath.lastIndexOf('/') + 1) || 'index.html';

  const navLinks = document.querySelectorAll('.nav-links a');
  navLinks.forEach((link) => {
    const href = link.getAttribute('href');
    if (href === currentPage) {
      link.classList.add('active');
    } else {
      link.classList.remove('active');
    }
  });

  // --------------------------------------------------------------------------
  // Entrance animations (staggered fade-in for hero/feature elements)
  // --------------------------------------------------------------------------
  const animateElements = document.querySelectorAll('.animate-in');
  if (animateElements.length > 0) {
    // Use IntersectionObserver for elements below the fold
    if ('IntersectionObserver' in window) {
      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              entry.target.style.animationPlayState = 'running';
              observer.unobserve(entry.target);
            }
          });
        },
        { threshold: 0.1, rootMargin: '0px 0px -40px 0px' }
      );

      animateElements.forEach((el) => {
        // Only observe elements that aren't already visible above fold
        const rect = el.getBoundingClientRect();
        if (rect.top > window.innerHeight) {
          el.style.animationPlayState = 'paused';
          observer.observe(el);
        }
      });
    }
  }

  // --------------------------------------------------------------------------
  // External link handling — open in new tab
  // --------------------------------------------------------------------------
  document.querySelectorAll('a[href^="http"]').forEach((link) => {
    if (!link.hasAttribute('target')) {
      link.setAttribute('target', '_blank');
      link.setAttribute('rel', 'noopener noreferrer');
    }
  });

  // --------------------------------------------------------------------------
  // Back to top button
  // --------------------------------------------------------------------------
  const backToTop = document.querySelector('.back-to-top');
  if (backToTop) {
    const toggleBackToTop = () => {
      if (window.scrollY > 400) {
        backToTop.classList.add('visible');
      } else {
        backToTop.classList.remove('visible');
      }
    };

    window.addEventListener('scroll', toggleBackToTop, { passive: true });
    toggleBackToTop();

    backToTop.addEventListener('click', () => {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  }

  // --------------------------------------------------------------------------
  // Copy button for code blocks
  // --------------------------------------------------------------------------
  document.querySelectorAll('pre').forEach((pre) => {
    const btn = document.createElement('button');
    btn.className = 'copy-btn';
    btn.setAttribute('aria-label', 'Copy code');
    btn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';

    btn.addEventListener('click', async () => {
      const code = pre.querySelector('code');
      const text = code ? code.textContent : pre.textContent;
      try {
        await navigator.clipboard.writeText(text);
        btn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>';
        btn.classList.add('copied');
        setTimeout(() => {
          btn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';
          btn.classList.remove('copied');
        }, 2000);
      } catch (err) {
        // Fallback: silent fail
      }
    });

    pre.style.position = 'relative';
    pre.appendChild(btn);
  });
});

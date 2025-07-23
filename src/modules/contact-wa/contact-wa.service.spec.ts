import { Test, TestingModule } from '@nestjs/testing';
import { ContactWaService } from './contact-wa.service';

describe('ContactWaService', () => {
  let service: ContactWaService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [ContactWaService],
    }).compile();

    service = module.get<ContactWaService>(ContactWaService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});

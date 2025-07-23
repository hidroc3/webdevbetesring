import { Test, TestingModule } from '@nestjs/testing';
import { CctvsService } from './cctvs.service';

describe('CctvsService', () => {
  let service: CctvsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [CctvsService],
    }).compile();

    service = module.get<CctvsService>(CctvsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
